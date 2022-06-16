package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.util.KeeperLogger;
import com.ctrip.xpipe.utils.FileUtils;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.slf4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public abstract class AbstractCommandStore extends AbstractStore implements CommandStore {

    private final static Logger delayTraceLogger = KeeperLogger.getDelayTraceLog();

    public static final long DEFAULT_COMMAND_READER_FLYING_THRESHOLD = 1 << 15;

    private final File baseDir;

    private final String fileNamePrefix;

    private final int maxFileSize;

    private final IntSupplier fileNumToKeep;
    private final int minTimeMilliToGcAfterModified;

    private final IntSupplier maxTimeSecondKeeperCmdFileAfterModified;

    private final FilenameFilter cmdFileFilter;

    private final FilenameFilter idxFileFilter;

    private final FilenameFilter allFileFilter;

    private final ConcurrentMap<CommandReader<?>, Boolean> readers = new ConcurrentHashMap<>();

    protected OffsetNotifier offsetNotifier;

    protected final long commandReaderFlyingThreshold;

    protected CommandStoreDelay commandStoreDelay;

    protected CommandReaderWriterFactory cmdReaderWriterFactory;

    private CommandWriter cmdWriter;

    private List<CommandFileOffsetGtidIndex> cmdIndexList = new CopyOnWriteArrayList<>();

    protected GtidSet baseGtidSet;

    private List<CommandsGuarantee> commandsGuarantees = new CopyOnWriteArrayList<>();

    private ReentrantLock gcLock = new ReentrantLock();

    private static final String INDEX_FILE_PREFIX = "idx_";

    private AtomicBoolean initialized = new AtomicBoolean(false);
    
    public abstract Logger getLogger();

    public AbstractCommandStore(File file, int maxFileSize, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
                               int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep,
                               long commandReaderFlyingThreshold, GtidSet baseGtidSet,
                               CommandReaderWriterFactory cmdReaderWriterFactory,
                               KeeperMonitor keeperMonitor) throws IOException {

        this.baseDir = file.getParentFile();
        this.fileNamePrefix = file.getName();
        this.maxFileSize = maxFileSize;
        this.maxTimeSecondKeeperCmdFileAfterModified = maxTimeSecondKeeperCmdFileAfterModified;
        this.fileNumToKeep = fileNumToKeep;
        this.commandReaderFlyingThreshold = commandReaderFlyingThreshold;
        this.minTimeMilliToGcAfterModified = minTimeMilliToGcAfterModified;
        this.cmdReaderWriterFactory = cmdReaderWriterFactory;
        this.commandStoreDelay = keeperMonitor.createCommandStoreDelay(this);
        this.baseGtidSet = baseGtidSet.clone();

        cmdFileFilter = new PrefixFileFilter(fileNamePrefix);
        idxFileFilter = new PrefixFileFilter(INDEX_FILE_PREFIX + fileNamePrefix);
        allFileFilter = new PrefixFileFilter(new String[] {fileNamePrefix, INDEX_FILE_PREFIX + fileNamePrefix});

        intiCmdFileIndex();
        cmdWriter = cmdReaderWriterFactory.createCmdWriter(this, maxFileSize, delayTraceLogger);
    }

    @Override
    public void initialize() throws Exception {
        if (initialized.compareAndSet(false, true)) {
            cmdWriter.initialize();
            offsetNotifier = new OffsetNotifier(cmdWriter.totalLength() - 1);
        }
    }


    @Override
    public void makeSureOpen() {
        super.makeSureOpen();
        if (!initialized.get()) {
            throw new IllegalStateException("[makeSureOpen][uninitialized]" + this);
        }
    }

    protected void intiCmdFileIndex() {
        File[] files = allIndexFiles();
        List<CommandFileOffsetGtidIndex> localIndexList = new LinkedList<>();
        for (File idxFile: files) {
            String cmdFileName = idxFile.getName().substring(INDEX_FILE_PREFIX.length());
            File file = new File(baseDir, cmdFileName);
            if (!file.exists()) {
                getLogger().info("[intiCmdFileIndex][{}] skip for no cmd file", idxFile);
                continue;
            }

            long startOffset = extractStartOffset(file);
            CommandFile commandFile = new CommandFile(file, startOffset);
            FileUtils.readFileAsStringLineByLine(idxFile, idxStr -> {
                CommandFileOffsetGtidIndex idx = CommandFileOffsetGtidIndex.createFromRawString(idxStr, commandFile);
                if (null != idx) localIndexList.add(idx);
            });
        }

        Collections.sort(localIndexList);
        this.cmdIndexList.addAll(localIndexList);
    }

    protected Logger getDelayTraceLogger() {
        return delayTraceLogger;
    }

    protected CommandStoreDelay getCommandStoreDelay() {
        return commandStoreDelay;
    }

    protected List<CommandFileOffsetGtidIndex> getIndexList() {
        return cmdIndexList;
    }

    protected CommandWriter getCmdWriter() {
        return cmdWriter;
    }

    protected GtidSet getBaseGtidSet() {
        return baseGtidSet;
    }

    private File fileForStartOffset(long startOffset) {
        return new File(baseDir, fileNamePrefix + startOffset);
    }

    private long findMaxStartOffset() {

        long maxStartOffset = 0;
        File[] files = allCmdFiles();
        if (files != null) {
            for (File file : files) {
                long startOffset = extractStartOffset(file);
                if (startOffset > maxStartOffset) {
                    maxStartOffset = startOffset;
                }
            }
        }
        return maxStartOffset;
    }

    @Override
    public File findIndexFile(CommandFile commandFile) {
        if (!commandFile.getFile().exists()) throw new IllegalArgumentException("command file must exist " + commandFile);
        return new File(baseDir, INDEX_FILE_PREFIX + commandFile.getFile().getName());
    }

    @Override
    public void addIndex(CommandFileOffsetGtidIndex index) {
        this.cmdIndexList.add(index);
    }

    private File[] allCmdFiles() {
        File []files = baseDir.listFiles(cmdFileFilter);
        if(files == null){
            files = new File[0];
        }
        return files;
    }

    private File[] allIndexFiles() {
        File []files = baseDir.listFiles(idxFileFilter);
        if(files == null){
            files = new File[0];
        }
        return files;
    }

    private File[] allFiles() {
        File []files = baseDir.listFiles(allFileFilter);
        if(files == null){
            files = new File[0];
        }
        return files;
    }

    private boolean delCmdFile(File cmdFile) {
        File idxFile = new File(baseDir, INDEX_FILE_PREFIX + cmdFile.getName());
        if (idxFile.exists()) {
            if (!idxFile.delete()) {
                getLogger().warn("[delCmdFile][{}] del idx file fail", idxFile);
            }

            this.cmdIndexList = cmdIndexList.stream()
                    .filter(index -> !index.getCommandFile().getFile().equals(cmdFile))
                    .collect(Collectors.toCollection(CopyOnWriteArrayList::new));
        }

        return cmdFile.delete();
    }

    private long extractStartOffset(File file) {
        return Long.parseLong(file.getName().substring(fileNamePrefix.length()));
    }

    @Override
    public int appendCommands(ByteBuf byteBuf) throws IOException {

        makeSureOpen();

        cmdWriter.rotateFileIfNecessary();

        commandStoreDelay.beginWrite();

        int wrote = cmdWriter.write(byteBuf);

        long offset = cmdWriter.totalLength() - 1;
        commandStoreDelay.endWrite(offset);

        offsetNotifier.offsetIncreased(offset);

        return wrote;
    }

    @Override
    public long totalLength() {
        return cmdWriter.totalLength();
    }

    public void rotateFileIfNecessary() throws IOException {
        cmdWriter.rotateFileIfNecessary();
    }

    public CommandFile findFileForOffset(long targetStartOffset) throws IOException {
        File[] files = baseDir.listFiles(cmdFileFilter);
        if (files != null) {
            for (File file : files) {
                long startOffset = extractStartOffset(file);
                if (targetStartOffset >= startOffset && (targetStartOffset < startOffset + file.length()
                        || targetStartOffset < startOffset + maxFileSize)) {
                    return new CommandFile(file, startOffset);
                }
            }
        }

        if (files != null) {
            for (File file : files) {
                getLogger().info("[findFileForOffset]{}, {}", file.getName(), file.length());
            }
        }
        return null;
    }

    @Override
    public CommandFile findLatestFile() throws IOException {
        long maxStartOffset = findMaxStartOffset();
        return new CommandFile(fileForStartOffset(maxStartOffset), maxStartOffset);
    }

    @Override
    public CommandFileSegment findLastFileSegment() throws IOException {
        if (this.cmdIndexList.isEmpty()) {
            CommandFileOffsetGtidIndex baseIndex = getBaseIndex();
            if (null == baseIndex) throw new IllegalArgumentException(); // TODO: handle fsync
            return new CommandFileSegment(baseIndex);
        } else {
            CommandFileOffsetGtidIndex lastIndex = this.cmdIndexList.get(cmdIndexList.size() - 1);
            return new CommandFileSegment(lastIndex);
        }
    }

    @Override
    public CommandFileSegment findFirstFileSegment(GtidSet excludedGtidSet) throws IOException {
        makeSureOpen();

        Set<String> interestedSrcIds = excludedGtidSet.getUUIDs();
        Iterator<CommandFileOffsetGtidIndex> indexIterator = cmdIndexList.iterator();
        CommandFileOffsetGtidIndex startIndex = getBaseIndex();
        if (null == startIndex) startIndex = indexIterator.next();
        CommandFileOffsetGtidIndex endIndex = null;

        GtidSet storeExcludedGtidSet = startIndex.getExcludedGtidSet().filterGtid(interestedSrcIds);
        if (!storeExcludedGtidSet.isContainedWithin(excludedGtidSet)) {
            // TODO: strictly
            throw new IllegalArgumentException("req cmd miss storeExcluded:" + storeExcludedGtidSet + " reqExcluded:" + excludedGtidSet);
        }

        while (indexIterator.hasNext()) {
            CommandFileOffsetGtidIndex index = indexIterator.next();
            GtidSet includedGtidSet = index.getExcludedGtidSet().filterGtid(interestedSrcIds)
                    .subtract(startIndex.getExcludedGtidSet());
            if (!includedGtidSet.isContainedWithin(excludedGtidSet)) {
                endIndex = index;
                if (!indexIterator.hasNext()) {
                    // right bound open
                    endIndex = null;
                }
            } else if (null == endIndex) {
                startIndex = index;
            } else {
                break;
            }
        }

        return new CommandFileSegment(startIndex, endIndex);
    }

    @Override
    public GtidSet getBeginGtidSet() throws IOException {
        CommandFileOffsetGtidIndex baseIndex = getBaseIndex();
        if (null != baseIndex) return baseIndex.getExcludedGtidSet();
        if (!cmdIndexList.isEmpty()) return cmdIndexList.get(0).getExcludedGtidSet();
        return new GtidSet("");
    }

    protected CommandFileOffsetGtidIndex getBaseIndex() throws IOException {
        CommandFile firstCommandFile = findFileForOffset(0L);
        if (null == firstCommandFile) return null;

        return new CommandFileOffsetGtidIndex(getBaseGtidSet(), firstCommandFile, 0);
    }

    public CommandFile findNextFile(File curFile) {
        if (!curFile.getParentFile().equals(baseDir)) {
            throw new IllegalArgumentException("file " + curFile + "not in dir " + baseDir);
        }

        long startOffset = extractStartOffset(curFile);
        long fileLength = curFile.length();
        if (0 == fileLength) return null;

        File nextFile = fileForStartOffset(startOffset + fileLength);
        if (nextFile.isFile()) {
            return new CommandFile(nextFile, extractStartOffset(nextFile));
        } else {
            return null;
        }
    }

    @Override
    public boolean awaitCommandsOffset(long offset, int timeMilli) throws InterruptedException {
        return offsetNotifier.await(offset, timeMilli);
    }

    @Override
    public long lowestReadingOffset() {
        long lowestReadingOffset = Long.MAX_VALUE;

        for (CommandReader reader : readers.keySet()) {
            File readingFile = reader.getCurFile();
            if (readingFile != null) {
                lowestReadingOffset = Math.min(lowestReadingOffset, extractStartOffset(readingFile));
            }
        }

        return lowestReadingOffset;
    }

    @Override
    public CommandFile newCommandFile(long startOffset) throws IOException {
        makeSureOpen();

        CommandFile commandFile = findFileForOffset(startOffset);
        if (null != commandFile) return commandFile;

        File newFile = new File(baseDir, fileNamePrefix + startOffset);
        return new CommandFile(newFile, startOffset);
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
            cmdWriter.close();
        }else{
            getLogger().warn("[close][already closed]{}", this);
        }
    }

    @Override
    public void destroy() throws Exception {

        getLogger().info("[destroy]{}", this);
        File [] files = allFiles();
        if(files != null){
            for(File file : files){
                boolean result = file.delete();
                getLogger().info("[destroy][delete file]{}, {}", file, result);
            }
        }
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

        long minCmdOffset = Long.MAX_VALUE; // start from zero
        File[] files = allCmdFiles();

        if (files == null || files.length == 0) {
            getLogger().info("[minCmdKeeperOffset][no cmd files][start offset 0]");
            minCmdOffset = 0L;
        } else {
            for (File cmdFile : files) {
                minCmdOffset = Math.min(extractStartOffset(cmdFile), minCmdOffset);
            }
        }
        return minCmdOffset;
    }

    @Override
    public boolean retainCommands(CommandsGuarantee commandsGuarantee) {
        try {
            gcLock.lock();
            long needCmdOffset = commandsGuarantee.getNeededCommandOffset();
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
            long offset = commandsGuarantee.getNeededCommandOffset();
            minOffset = Long.min(offset, minOffset);
        }

        return minOffset;
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

            for (File cmdFile : allCmdFiles()) {
                long fileStartOffset = extractStartOffset(cmdFile);
                if (canDeleteCmdFile(Long.min(lowestReadingOffset(), minGuaranteeOffset()), fileStartOffset, cmdFile.length(),
                        cmdFile.lastModified())) {
                    getLogger().info("[GC] delete command file {}", cmdFile);
                    delCmdFile(cmdFile);
                }
            }
        } finally {
            gcLock.unlock();
        }
    }

    protected boolean canDeleteCmdFile(long lowestReadingOffset, long fileStartOffset, long fileSize, long lastModified) {

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
        long totalKeep = (long)fileSize * fileNumToKeep.getAsInt();
        boolean fileKeep = totalLength - (fileStartOffset + fileSize) > totalKeep;

        getLogger().debug("[canDeleteCmdFile][fileKeep]{}, {} - {} > {}({}*{})", fileKeep, totalLength, (fileStartOffset + fileSize), totalKeep, fileSize, fileNumToKeep);
        if(!fileKeep){
            return false;
        }
        return true;
    }

}
