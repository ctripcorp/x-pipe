package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.cmd.OffsetNotifyingCommandWriter;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.DefaultIndexStore;
import com.ctrip.xpipe.redis.keeper.store.gtid.index.TimerSlidingWindow;
import com.ctrip.xpipe.redis.keeper.util.KeeperLogger;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK_V2;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX_V2;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public abstract class AbstractCommandStore extends AbstractStore implements CommandStore, CommandWriterCallback, AsyncCommandStore {

    private final static Logger delayTraceLogger = KeeperLogger.getDelayTraceLog();

    public static final long DEFAULT_COMMAND_READER_FLYING_THRESHOLD = 1 << 15;

    private final File baseDir;

    private final String fileNamePrefix;

    private final int maxFileSize;

    private final IntSupplier fileNumToKeep;
    private final int minTimeMilliToGcAfterModified;

    private final IntSupplier maxTimeSecondKeeperCmdFileAfterModified;

    private final ConcurrentMap<CommandReader<?>, Boolean> readers = new ConcurrentHashMap<>();

    protected OffsetNotifier offsetNotifier;

    private final BooleanSupplier commandOffsetNotifyCoalescingEnabled;

    protected final long commandReaderFlyingThreshold;

    protected CommandStoreDelay commandStoreDelay;

    protected CommandStoreDelay indexStoreDelay;

    protected CommandReaderWriterFactory cmdReaderWriterFactory;

    private CommandWriter cmdWriter;

    protected GtidSet baseGtidSet;

    private List<CommandsGuarantee> commandsGuarantees = new CopyOnWriteArrayList<>();

    private ReentrantLock gcLock = new ReentrantLock();

    private static final String INDEX_FILE_PREFIX = "idx_";

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private AtomicReference<SyncRateLimiter> rateLimiterRef = new AtomicReference<>();

    private IndexStore indexStore;

    private RedisOpParser redisOpParser;

    private GtidCmdFilter gtidCmdFilter;

    private boolean buildIndex;

    private CKStore ckStore;

    private KeeperConfig keeperConfig;

    private TimerSlidingWindow timerSlidingWindow;

    protected final AsyncFileSystem asyncFileSystem;

    protected final AsyncSegmentFile asyncSegmentFile;

    private final List<String> commandIndexPrefixes;

    private final ReplId fileSystemReplId;

    private final IntSupplier asyncWriteMaxBytes;
    
    public abstract Logger getLogger();

    public AbstractCommandStore(CKStore ckStore, KeeperConfig keeperConfig, File file, int maxFileSize, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
                                int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep,
                                long commandReaderFlyingThreshold,
                                BooleanSupplier commandOffsetNotifyCoalescingEnabled,
                                CommandReaderWriterFactory cmdReaderWriterFactory,
                                KeeperMonitor keeperMonitor, RedisOpParser redisOpParser,
                                GtidCmdFilter  gtidCmdFilter, boolean buildIndex, long cmdStoreStartOffset,
                                AsyncFileSystem asyncFileSystem,
                                IntSupplier asyncWriteMaxBytes,
                                ReplId fileSystemReplId
    ) throws IOException {

        this.baseDir = file.getParentFile();
        this.fileNamePrefix = file.getName();
        this.maxFileSize = maxFileSize;
        this.asyncFileSystem = Objects.requireNonNull(asyncFileSystem, "asyncFileSystem");
        this.fileSystemReplId = Objects.requireNonNull(fileSystemReplId, "fileSystemReplId");
        this.asyncWriteMaxBytes = asyncWriteMaxBytes == null ? () -> DEFAULT_ASYNC_WRITE_MAX_BYTES : asyncWriteMaxBytes;
        this.maxTimeSecondKeeperCmdFileAfterModified = maxTimeSecondKeeperCmdFileAfterModified;
        this.fileNumToKeep = fileNumToKeep;
        this.commandReaderFlyingThreshold = commandReaderFlyingThreshold;
        this.commandOffsetNotifyCoalescingEnabled = commandOffsetNotifyCoalescingEnabled;
        this.minTimeMilliToGcAfterModified = minTimeMilliToGcAfterModified;
        this.cmdReaderWriterFactory = cmdReaderWriterFactory;
        this.commandStoreDelay = keeperMonitor.createCommandStoreDelay(this);
        this.indexStoreDelay = keeperMonitor.createCommandStoreDelay(this);
        this.redisOpParser = redisOpParser;
        this.gtidCmdFilter = gtidCmdFilter;
        this.ckStore = ckStore;
        this.keeperConfig = keeperConfig != null ? keeperConfig
                : (ckStore != null ? ckStore.getKeeperConfig() : null);

        // T-X1a.2: expand from V1-only (index_/block_) to include V2 (indexv2_/blockv2_).
        // fs.open must register all 4 index prefixes so that segment truncate/delete keeps
        // both V1 and V2 index/block files consistent with the cmd segment.
        this.commandIndexPrefixes = Arrays.asList(
                INDEX + fileNamePrefix,
                BLOCK + fileNamePrefix,
                INDEX_V2 + fileNamePrefix,
                BLOCK_V2 + fileNamePrefix);
        this.asyncSegmentFile = AsyncFileSystemHelper.awaitOpen(asyncFileSystem,
                asyncFileSystem.open(baseDir.getAbsolutePath(), fileNamePrefix, commandIndexPrefixes, true, fileSystemReplId.toString()),
                "open command segment " + fileNamePrefix);
        // invalid 文件列表见 T-FS.2；FS initFromFiles 内部已 warn，Store 待 FS 暴露 invalidFiles() 后再补日志

        cmdWriter = cmdReaderWriterFactory.createCmdWriter(this, this.maxFileSize, delayTraceLogger);
        this.buildIndex = buildIndex;
        indexStore = createIndexStore(cmdStoreStartOffset);
    }

    private IndexStore createIndexStore(long cmdStoreStartOffset) {
        return new DefaultIndexStore(keeperConfig, ckStore, this, baseDir.getAbsolutePath(), redisOpParser,
                this, gtidCmdFilter, cmdStoreStartOffset);
    }

    @Override
    public int writeCommand(ByteBuf commandBuf) throws IOException {
        return onlyAppendCommand(commandBuf);
    }

    @Override
    public long getCurrentOffset() {
        return cmdWriter.totalLength() - 1;
    }

    @Override
    public long getCmdFileLen() {
        if (null == cmdWriter) return -1;
        return cmdWriter.fileLength();
    }

    @Override
    public int getPendingSize(){
        if(timerSlidingWindow != null){
            return timerSlidingWindow.bufferSize();
        }
        return 0;
    }


    @Override
    public CommandWriter getCommandWriter() {
        return cmdWriter;
    }

    @Override
    public void flushPendingData() throws IOException {
        makeSureOpen();
        flushSlidingWindow();
        if (indexStore != null) {
            indexStore.flushWriter();
        }
    }

    @Override
    public void flushSlidingWindow() throws IOException{
        if(timerSlidingWindow != null){
            timerSlidingWindow.flushAll();
        }
    }


    @Override
    public void initialize() throws IOException {
        if (initialized.compareAndSet(false, true)) {
            cmdWriter.initialize();
            offsetNotifier = new OffsetNotifier(cmdWriter.totalLength() - 1);
            if (cmdWriter instanceof OffsetNotifyingCommandWriter) {
                ((OffsetNotifyingCommandWriter) cmdWriter).setOffsetNotifier(offsetNotifier);
            }
            if(buildIndex) {
                indexStore.openWriter(cmdWriter);
            }
            if(ckStore != null) {
                this.timerSlidingWindow = new TimerSlidingWindow(ckStore.getKeeperConfig(), cmdWriter, commandStoreDelay, offsetNotifier, ckStore.getMasterEventLoop());
            }
        }
    }


    @Override
    public void makeSureOpen() {
        super.makeSureOpen();
        if (!initialized.get()) {
            throw new IllegalStateException("[makeSureOpen][uninitialized]" + this);
        }
    }

    protected Logger getDelayTraceLogger() {
        return delayTraceLogger;
    }

    protected CommandStoreDelay getCommandStoreDelay() {
        return commandStoreDelay;
    }

    @Override
    public void attachRateLimiter(SyncRateLimiter rateLimiter) {
        this.rateLimiterRef.set(rateLimiter);
    }

    @Override
    public int appendCommands(ByteBuf byteBuf) throws IOException {

        makeSureOpen();

        rotateFileIfNecessary();

        if(buildIndex) {
            return appendCommandsWithIndex(byteBuf);
        } else {
            return onlyAppendCommand(byteBuf);
        }
    }

    @Override
    public int onlyAppendCommand(ByteBuf byteBuf) throws IOException {

        SyncRateLimiter rateLimiter = rateLimiterRef.get();

        if (null != rateLimiter) rateLimiter.acquire(byteBuf.readableBytes());

        if(timerSlidingWindow != null && commandOffsetNotifyCoalescingEnabled.getAsBoolean()){
            return timerSlidingWindow.write(byteBuf,buildIndex);
        }

        if(timerSlidingWindow != null) {
            timerSlidingWindow.flushAll();
        }

        commandStoreDelay.beginWrite();

        int wrote = cmdWriter.write(byteBuf);

        long offset = cmdWriter.totalLength() - 1;

        commandStoreDelay.endWrite(offset);

        if (!(cmdWriter instanceof OffsetNotifyingCommandWriter)) {
            offsetNotifier.offsetIncreased(offset);
        }



        return wrote;
    }

    private int appendCommandsWithIndex(ByteBuf byteBuf) throws IOException {

        indexStoreDelay.beginWrite();

        long beginOffset = cmdWriter.totalLength() - 1;
        indexStore.write(byteBuf);
        long offset = cmdWriter.totalLength() - 1;

        indexStoreDelay.endWrite(offset);

        int writer = (int)(offset - beginOffset);
        return writer;
    }


    @Override
    public long totalLength() {
        return cmdWriter.totalLength();
    }

    public void rotateFileIfNecessary() throws IOException {
        if(cmdWriter.needRotate()) {
            flushSlidingWindow();
            if(!buildIndex) {
                cmdWriter.doRotate();
                return;
            }
            if (indexStore.needRotate()) {
                // Atomic under IndexStore monitor: flush → cmd roll → doSwitchCmdFile (spec §3.7.7 P0-1)
                indexStore.rotateWithCmdRoll(() -> {
                    cmdWriter.doRotate();
                    return null;
                });
            }
        }

    }

    @Override
    public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
        return offsetNotifier.await(offset, timeMilli);
    }

    @Override
    public long lowestReadingOffset() {
        long lowestReadingOffset = Long.MAX_VALUE;

        for (CommandReader<?> reader : readers.keySet()) {
            // Reader owns logical read cursor; transferTo does not advance AsyncSegmentFile.position.
            long readingOffset = reader.getReadOffset();
            if (readingOffset >= 0) {
                lowestReadingOffset = Math.min(lowestReadingOffset, readingOffset);
            }
        }

        return lowestReadingOffset;
    }

    @Override
    public void addReader(CommandReader<?> reader) {
        this.readers.put(reader, Boolean.TRUE);
    }

    @Override
    public void removeReader(CommandReader<?> reader) {
        this.readers.remove(reader);
    }

    @Override
    public void close() throws IOException {

        if(cmpAndSetClosed()){
            getLogger().info("[close]{}", this);

            if(timerSlidingWindow != null) {
                timerSlidingWindow.close();
            }

            cmdWriter.close();
            if(indexStore != null) {
                // Terminal close (AbstractStore); index AsyncFile released with segment below.
                indexStore.close();
            }
            AsyncFileSystemHelper.await(asyncFileSystem.close(asyncSegmentFile), "close command segment " + fileNamePrefix);
        }else{
            getLogger().warn("[close][already closed]{}", this);
        }
    }

    @Override
    public AsyncFileSystem getAsyncFileSystem() {
        return asyncFileSystem;
    }

    @Override
    public AsyncSegmentFile getAsyncSegmentFile() {
        return asyncSegmentFile;
    }

    @Override
    public AsyncSegmentFile getWriteSegmentFile() {
        return asyncSegmentFile;
    }

    /**
     * Index-only tail truncate for the current write segment (spec §3.7.3). Callers pass the V1 or V2
     * prefix pair depending on which writer is recovering. Cmd segment position is untouched.
     * <p>T-X1a.5 lands the API only — no caller is wired up until T-X1c/T-X1d.
     */
    @Override
    public Map<String, AsyncFile> truncateIndex(String indexPrefix, String blockPrefix,
                                                long indexSize, long blockSize) throws IOException {
        List<String> prefixes = Arrays.asList(indexPrefix, blockPrefix);
        Map<String, AsyncFile> handles = AsyncFileSystemHelper.await(
                asyncFileSystem.getCurrentIndexFiles(asyncSegmentFile, prefixes),
                "getCurrentIndexFiles for truncateIndex " + indexPrefix + "/" + blockPrefix).getValue();
        AsyncFile indexFile = handles.get(indexPrefix);
        AsyncFile blockFile = handles.get(blockPrefix);
        if (indexFile == null || blockFile == null) {
            throw new IOException("[truncateIndex] missing index/block handle for " + indexPrefix + "/" + blockPrefix);
        }
        AsyncFileSystemHelper.await(asyncFileSystem.truncate(indexFile, indexSize),
                "truncate " + indexPrefix + " to " + indexSize);
        AsyncFileSystemHelper.await(asyncFileSystem.truncate(blockFile, blockSize),
                "truncate " + blockPrefix + " to " + blockSize);
        return handles;
    }

    /**
     * Cmd-only tail truncate for the write segment (spec §3.7.3). Companion index/block file contents
     * are NOT modified by FS truncate — callers roll their own {@link #truncateIndex} follow-up.
     * After truncate, re-fetches write index handles via {@code getCurrentIndexFiles} (FS truncate
     * itself returns void as of commit 6c82c2c).
     */
    @Override
    public Map<String, AsyncFile> truncateCmdSegment(long cmdSegmentOffset) throws IOException {
        long globalOffset = getCurrentSegmentStartOffset() + cmdSegmentOffset;
        AsyncFileSystemHelper.await(
                asyncFileSystem.truncate(asyncSegmentFile, globalOffset),
                "truncate cmd segment to " + globalOffset);
        return AsyncFileSystemHelper.await(
                asyncFileSystem.getCurrentIndexFiles(asyncSegmentFile),
                "getCurrentIndexFiles after truncateCmdSegment " + globalOffset).getValue();
    }

    @Override
    public long getCurrentSegmentStartOffset() throws IOException {
        long startOffset = asyncFileSystem.getCurrentSegmentStartOffset(asyncSegmentFile);
        if (startOffset < 0) {
            List<Long> offsets = asyncFileSystem.list(asyncSegmentFile);
            startOffset = offsets.isEmpty() ? 0 : offsets.get(offsets.size() - 1);
        }
        return startOffset;
    }

    @Override
    public File getCommandBaseDir() {
        return baseDir;
    }

    @Override
    public String getCommandFileNamePrefix() {
        return fileNamePrefix;
    }

    @Override
    public List<String> getCommandIndexPrefixes() {
        return commandIndexPrefixes;
    }

    @Override
    public ReplId getFileSystemReplId() {
        return fileSystemReplId;
    }

    @Override
    public int getAsyncWriteMaxBytes() {
        return Math.max(1, asyncWriteMaxBytes.getAsInt());
    }

    @Override
    public long currentSegmentSize() throws IOException {
        if (cmdWriter == null) {
            throw new IOException("cmd writer not initialized");
        }
        return cmdWriter.fileLength();
    }

    @Override
    public void destroy() throws Exception {

        getLogger().info("[destroy]{}", this);
        close();
        AsyncFileSystemHelper.await(asyncFileSystem.delete(asyncSegmentFile),
                "destroy command segment " + fileNamePrefix);
    }

    @Override
    public String toString() {
        return String.format("CommandStore:%s", baseDir);
    }

    public String simpleDesc(){

        File desc1 = baseDir.getParentFile();
        File desc2 = null;
        if(desc1 != null){
            desc2 = desc1.getParentFile();
        }
        return String.format("%s.%s",
                desc2 == null?null:desc2.getName(),
                desc1 == null?null:desc1.getName());
    }

    @Override
    public long lowestAvailableOffset() {
        List<Long> segmentOffsets = asyncFileSystem.list(asyncSegmentFile);
        if (segmentOffsets == null || segmentOffsets.isEmpty()) {
            getLogger().info("[lowestAvailableOffset][no cmd segments][start offset 0]");
            return 0L;
        }
        // fs.list returns ascending startOffsets (T-FS.15)
        return segmentOffsets.get(0);
    }

    @Override
    public boolean retainCommands(CommandsGuarantee commandsGuarantee) {
        try {
            gcLock.lock();
            long needCmdOffset = commandsGuarantee.getBacklogOffset();
            long minOffset = lowestAvailableOffset();
            if (minOffset <= needCmdOffset) {
                this.commandsGuarantees.add(commandsGuarantee);
                return true;
            }
        } finally {
            gcLock.unlock();
        }

        return false;
    }

    private void timeoutGuarantees() {
        List<CommandsGuarantee> timeoutGuarantees = commandsGuarantees.stream().filter(CommandsGuarantee::isTimeout).collect(Collectors.toList());
        commandsGuarantees.removeAll(timeoutGuarantees);
    }

    private void finishGuarantees() {
        List<CommandsGuarantee> finishGuarantees = commandsGuarantees.stream().filter(CommandsGuarantee::isFinish).collect(Collectors.toList());
        commandsGuarantees.removeAll(finishGuarantees);
    }

    private long minGuaranteeOffset() {
        long minOffset = Long.MAX_VALUE;
        for (CommandsGuarantee commandsGuarantee : commandsGuarantees) {
            long offset = commandsGuarantee.getBacklogOffset();
            minOffset = Long.min(offset, minOffset);
        }

        return minOffset;
    }

    @Override
    public List<BacklogOffsetReplicationProgress> locateCmdSegment(String uuid, long begGno, long endGno) throws IOException {
        if (null == indexStore) {
            return Collections.emptyList();
        }

        List<Pair<Long, Long>> backlogOffsetRanges = indexStore.locateGtidRange(uuid, begGno, endGno);
        List<BacklogOffsetReplicationProgress> cmdSegments = new ArrayList<>();
        for (Pair<Long, Long> range : backlogOffsetRanges) {
            cmdSegments.add(new BacklogOffsetReplicationProgress(range.getKey(), range.getValue()));
        }
        return cmdSegments;
    }

    @Override
    public long getCommandsLastUpdatedAt() {
        return cmdWriter.getFileLastModified();
    }

    @Override
    public void gc() {
        try {
            gcLock.lock();
            timeoutGuarantees();
            finishGuarantees();

            List<Long> segmentOffsets = asyncFileSystem.list(asyncSegmentFile);
            if (segmentOffsets == null || segmentOffsets.size() <= 1) {
                getLogger().debug("[gc][no candidate segment] {}", segmentOffsets);
                return;
            }
            int totalSegments = segmentOffsets.size();
            long lowestReadOrGuarantee = Long.min(lowestReadingOffset(), minGuaranteeOffset());

            List<Long> toDelete = new ArrayList<>();
            for (int idx = 0; idx < totalSegments - 1; idx++) {
                long startOffset = segmentOffsets.get(idx);
                long size;
                long lastModified;
                try {
                    size = AsyncFileSystemHelper.await(
                            asyncFileSystem.sizeOfSegment(asyncSegmentFile, startOffset),
                            "size of segment " + fileNamePrefix + startOffset);
                    lastModified = AsyncFileSystemHelper.await(
                            asyncFileSystem.lastModifiedOfSegment(asyncSegmentFile, startOffset),
                            "last modified of segment " + fileNamePrefix + startOffset);
                } catch (IOException e) {
                    getLogger().error("[gc][stat segment {}]", startOffset, e);
                    break;
                }

                if (!canDeleteSegment(lowestReadOrGuarantee, startOffset, size, lastModified, idx, totalSegments)) {
                    break; // must delete a contiguous prefix
                }
                toDelete.add(startOffset);
            }

            if (toDelete.isEmpty()) {
                return;
            }
            getLogger().info("[gc][delete segments] {}", toDelete);
            try {
                AsyncFileSystemHelper.await(asyncFileSystem.deleteSegments(asyncSegmentFile, toDelete),
                        "delete segments " + toDelete);
            } catch (IOException e) {
                getLogger().error("[gc][deleteSegments {}]", toDelete, e);
            }
        } finally {
            gcLock.unlock();
        }
    }

    protected boolean canDeleteSegment(long lowestReadOrGuarantee, long startOffset, long size, long lastModified,
                                       int idx, int totalSegments) {
        getLogger().debug("[canDeleteSegment] start:{} size:{} idx:{} total:{}", startOffset, size, idx, totalSegments);

        boolean lowestReading = (startOffset + size < lowestReadOrGuarantee);
        getLogger().debug("[canDeleteSegment][lowestReading]{}, {}+{}<{}", lowestReading, startOffset, size, lowestReadOrGuarantee);
        if (!lowestReading) {
            return false;
        }

        long now = System.currentTimeMillis();
        long age = now - lastModified;
        long maxMilliKeepCmd = TimeUnit.SECONDS.toMillis(maxTimeSecondKeeperCmdFileAfterModified.getAsInt());

        getLogger().debug("[canDeleteSegment][age]{} min:{} max:{}", age, minTimeMilliToGcAfterModified, maxMilliKeepCmd);
        if (age < minTimeMilliToGcAfterModified) {
            return false;
        }
        if (age > maxMilliKeepCmd) {
            return true;
        }

        int newerCount = totalSegments - 1 - idx - 1; // exclude writing (last) segment
        boolean fileKeep = newerCount > fileNumToKeep.getAsInt();
        getLogger().debug("[canDeleteSegment][fileKeep]{}, newer:{} keep:{}", fileKeep, newerCount, fileNumToKeep.getAsInt());
        return fileKeep;
    }

    @Deprecated
    protected boolean canDeleteCmdFile(long lowestReadingOffset, long fileStartOffset, long fileSize, long lastModified) {
        getLogger().debug("[canDeleteCmdFile] start from {}", fileStartOffset);

        boolean lowestReading = (fileStartOffset + fileSize < lowestReadingOffset);

        getLogger().debug("[canDeleteCmdFile][lowestReading]{}, {}+{}<{}", lowestReading, fileStartOffset, fileSize, lowestReadingOffset);
        if(!lowestReading){
            return false;
        }

        Date now = new Date();
        long maxMilliKeepCmd = TimeUnit.SECONDS.toMillis(maxTimeSecondKeeperCmdFileAfterModified.getAsInt());
        boolean time = now.getTime() - lastModified >= minTimeMilliToGcAfterModified;
        boolean fresh = now.getTime() - lastModified <= maxMilliKeepCmd;

        getLogger().debug("[canDeleteCmdFile][time]{}, {} - {} > {}", time, now, new Date(lastModified), minTimeMilliToGcAfterModified);
        if(!time){
            return false;
        }
        getLogger().debug("[canDeleteCmdFile][fresh]{}, {} - {} < {}", fresh, now, new Date(lastModified), maxMilliKeepCmd);
        if (!fresh) {
            return true;
        }

        long totalLength = totalLength();
        long totalKeep = fileSize * fileNumToKeep.getAsInt();
        boolean fileKeep = totalLength - (fileStartOffset + fileSize) > totalKeep;

        getLogger().debug("[canDeleteCmdFile][fileKeep]{}, {} - {} > {}({}*{})", fileKeep, totalLength, (fileStartOffset + fileSize), totalKeep, fileSize, totalKeep);
        if(!fileKeep){
            return false;
        }
        return true;
    }

    @Override
    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet gtidSet) throws IOException {
        return indexStore.locateContinueGtidSet(gtidSet);
    }

    @Override
    public Pair<Long, GtidSet> locateContinueGtidSetWithFallbackToEnd(GtidSet gtidSet) throws IOException {
        return indexStore.locateGtidSetWithFallbackToEnd(gtidSet);
    }

    @Override
    public Pair<Long, GtidSet> locateTailOfCmd() {
        if(indexStore != null) {
            return this.indexStore.locateTailOfCmd();
        }
        return null;
    }

    @Override
    public GtidSet getIndexGtidSet() {
        if(indexStore == null) {
            throw new IllegalStateException("indexStore is null");
        }
        return indexStore.getIndexGtidSet();
    }

    @Override
    public synchronized void switchToXSync(GtidSet gtidSet) throws IOException {
        if (buildIndex) {
            return;
        }
        flushSlidingWindow();
        if (indexStore != null) {
            indexStore.closeWriter();
        }
        AsyncFileSystemHelper.await(asyncFileSystem.roll(asyncSegmentFile), "roll on switchToXSync");
        long newCmdStoreStartOffset = getCurrentSegmentStartOffset();
        getLogger().info("[switchToXSync] new cmdStoreStartOffset={}", newCmdStoreStartOffset);
        indexStore = createIndexStore(newCmdStoreStartOffset);
        indexStore.openWriter(cmdWriter);
        buildIndex = true;
    }

    @Override
    public synchronized void switchToPsync(String replId, long offset) throws IOException {
        if(!buildIndex)return;
        buildIndex = false;
        flushSlidingWindow();
        if(indexStore != null) {
            indexStore.closeWriter();
        }
    }

    @Override
    public boolean increaseLostNotInCmdStore(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException {
        return indexStore.increaseLost(lost, supplier);
    }

    @Override
    public void resetStateForContinue() {
        if(indexStore != null) {
            indexStore.resetParserState();
        }
    }

}
