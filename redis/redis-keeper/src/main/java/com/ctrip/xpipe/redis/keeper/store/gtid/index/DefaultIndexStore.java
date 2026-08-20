package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.utils.IOSupplier;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpItem;
import com.ctrip.xpipe.redis.core.redis.operation.stream.StreamTransactionListener;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.core.store.CommandWriterCallback;
import com.ctrip.xpipe.redis.core.store.GtidCmdFilter;
import com.ctrip.xpipe.redis.core.store.IndexStore;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.replication.LostGtidsetBacklogConflictException;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFile;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystemHelper;
import com.ctrip.xpipe.redis.keeper.storage.AsyncSegmentFile;
import com.ctrip.xpipe.redis.keeper.store.AbstractStore;
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK_V2;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX_V2;

public class DefaultIndexStore extends AbstractStore implements IndexStore, StreamTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultIndexStore.class);

    private IndexWriter indexWriter;
    private IndexWriterV2 indexWriterV2;
    private StreamCommandReader streamCommandReader;

    private final String baseDir;
    private final AsyncCommandStore asyncCommandStore;
    private final AsyncFileSystem fs;
    private final ReplId replId;

    private final RedisOpParser opParser;
    private GtidSet startGtidSet;
    /**
     * Snapshot of continue GtidSet taken immediately before rotate-failure unbind (T-H3.CP-R.1).
     * {@link #resolveContinueGtidSet()} uses this when writers are null; never falls back to
     * {@link #startGtidSet} (empty constructor value).
     */
    private GtidSet continueGtidSetSnapshot;
    private final CommandWriterCallback commandWriterCallback;
    private final GtidCmdFilter gtidCmdFilter;
    private boolean writerCmdEnabled;
    private final CKStore ckStore;
    private final KeeperConfig keeperConfig;
    private long cmdStoreStartOffset;

    /**
     * Test hook: runs after {@link #publishLocateSnapshot()} (reader already init'd) releases the monitor
     * and before lock-free {@code seek}. Production leaves this null.
     */
    volatile Runnable afterLocateSnapshotHook;

    /**
     * Point-in-time tip published under the IndexStore monitor (spec §3.7.10).
     * {@link #reader} is already {@code init}'d under the same lock as rotate; only {@code seek} runs lock-free.
     * {@code -1} Fallback uses frozen tail, not live {@link #locateTailOfCmd()}.
     */
    static final class LocateSnapshot implements Closeable {
        final IndexReader reader;
        final long tipSegmentStart;
        final long tailBacklogOffset;
        final GtidSet tailGtidSet;
        final long cmdStoreStartOffset;

        LocateSnapshot(IndexReader reader, long tipSegmentStart, long tailBacklogOffset,
                       GtidSet tailGtidSet, long cmdStoreStartOffset) {
            this.reader = reader;
            this.tipSegmentStart = tipSegmentStart;
            this.tailBacklogOffset = tailBacklogOffset;
            this.tailGtidSet = tailGtidSet;
            this.cmdStoreStartOffset = cmdStoreStartOffset;
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public DefaultIndexStore(KeeperConfig keeperConfig, CKStore ckStore, AsyncCommandStore asyncCommandStore,
                             String baseDir, RedisOpParser redisOpParser,
                             CommandWriterCallback commandWriterCallback, GtidCmdFilter gtidCmdFilter) {
        this(keeperConfig, ckStore, asyncCommandStore, baseDir, redisOpParser,
                commandWriterCallback, gtidCmdFilter, 0L);
    }

    public DefaultIndexStore(KeeperConfig keeperConfig, CKStore ckStore, AsyncCommandStore asyncCommandStore,
                             String baseDir, RedisOpParser redisOpParser,
                             CommandWriterCallback commandWriterCallback, GtidCmdFilter gtidCmdFilter,
                             long cmdStoreStartOffset) {
        this.baseDir = baseDir;
        this.asyncCommandStore = asyncCommandStore;
        this.fs = asyncCommandStore.getAsyncFileSystem();
        this.replId = asyncCommandStore.getFileSystemReplId();
        this.opParser = redisOpParser;
        this.commandWriterCallback = commandWriterCallback;
        this.startGtidSet = new GtidSet("");
        this.gtidCmdFilter = gtidCmdFilter;
        this.writerCmdEnabled = true;
        this.keeperConfig = keeperConfig;
        this.ckStore = ckStore;
        this.cmdStoreStartOffset = cmdStoreStartOffset;
    }

    public long getCmdStoreStartOffset() {
        return Math.max(0L, cmdStoreStartOffset);
    }

    public AsyncCommandStore getAsyncCommandStore() {
        return asyncCommandStore;
    }

    @Override
    public void openWriter(CommandWriter cmdWriter) throws IOException {
        makeSureOpen();
        this.streamCommandReader = new StreamCommandReader(this, cmdWriter.fileLength());
        openWritersWithHandles(getWriteIndexHandles(keeperConfig.dualWrite()), startGtidSet);
    }

    private void openWritersWithHandles(Map<String, AsyncFile> writeHandles, GtidSet headerGtidSet) throws IOException {
        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        List<String> prefixes = writerIndexPrefixes(keeperConfig.dualWrite());

        AsyncSegmentFile recoverSeg = openReadSegment(prefixes);
        try {
            long segStart = asyncCommandStore.getCurrentSegmentStartOffset();
            AsyncFileSystemHelper.await(() -> fs.position(recoverSeg, segStart), "position recover segment");
            Map<String, AsyncFile> readHandles = AsyncFileSystemHelper.await(() -> fs.getCurrentIndexFiles(recoverSeg, prefixes), "get read index handles for recover").getValue();

            GtidSet v2HeaderGtidSet = headerGtidSet;
            if (keeperConfig.dualWrite()) {
                this.indexWriter = new IndexWriter(headerGtidSet.clone(), this);
                AsyncFile readIndex = readHandles.get(INDEX + prefix);
                if (readIndex != null && indexFileNeedsRecover(readIndex)) {
                    indexWriter.recoverIndex(readIndex, readHandles.get(BLOCK + prefix));
                }
                v2HeaderGtidSet = this.indexWriter.getGtidSet();
            }
            this.indexWriterV2 = new IndexWriterV2(v2HeaderGtidSet.clone(), this,
                    keeperConfig.getIndexZoneConsecutiveThreshold(),
                    keeperConfig.getIndexMixedTotalBytesThreshold(),
                    keeperConfig.getBlockSizeThreshold());
            AsyncFile readIndexV2 = readHandles.get(INDEX_V2 + prefix);
            if (readIndexV2 != null && indexFileNeedsRecover(readIndexV2)) {
                indexWriterV2.recoverIndex(readIndexV2, readHandles.get(BLOCK_V2 + prefix));
            }
        } finally {
            AsyncFileSystemHelper.closeReadHandle(fs, recoverSeg, "close recover segment");
        }

        if (keeperConfig.dualWrite()) {
            indexWriter.init(writeHandles.get(INDEX + prefix), writeHandles.get(BLOCK + prefix));
        }
        indexWriterV2.init(writeHandles.get(INDEX_V2 + prefix), writeHandles.get(BLOCK_V2 + prefix));
    }

    private boolean indexFileNeedsRecover(AsyncFile readIndexFile) throws IOException {
        return AsyncFileSystemHelper.await(() -> fs.size(readIndexFile), "size index for recover check") > 0;
    }

    private Map<String, AsyncFile> getWriteIndexHandles(boolean dualWrite) throws IOException {
        List<String> prefixes = writerIndexPrefixes(dualWrite);
        return AsyncFileSystemHelper.await(() -> fs.getCurrentIndexFiles(asyncCommandStore.getWriteSegmentFile(), prefixes),
                "get write index handles").getValue();
    }

    private List<String> writerIndexPrefixes(boolean dualWrite) {
        String p = asyncCommandStore.getCommandFileNamePrefix();
        if (dualWrite) {
            return List.of(INDEX + p, BLOCK + p, INDEX_V2 + p, BLOCK_V2 + p);
        }
        return List.of(INDEX_V2 + p, BLOCK_V2 + p);
    }

    public void switchCmdFile(CommandWriter cmdWriter) throws IOException {
        doSwitchCmdFile();
    }

    /**
     * Rebind writers to the current write segment (already rolled by CmdStore).
     * Callers must {@link #flushWriter()} before {@code fs.roll} so pending index is
     * persisted on the old segment (V1 truncate-then-write still needs open channels;
     * spec §3.7.7). Parser state is preserved across rotate — only {@link #closeWriter()}
     * resets it (protocol switch); store teardown uses {@link #close()}.
     */
    public synchronized void doSwitchCmdFile() throws IOException {
        makeSureOpen();
        GtidSet continueGtidSet = resolveContinueGtidSet();
        openWritersWithHandles(getWriteIndexHandles(keeperConfig.dualWrite()), continueGtidSet);
        this.streamCommandReader.resetOffset();
        logger.info("[switchCmdFile] index_store switch to segment {}", asyncCommandStore.getCurrentSegmentStartOffset());
    }

    private GtidSet resolveContinueGtidSet() {
        if (indexWriterV2 != null && keeperConfig.readV2()) {
            return indexWriterV2.getGtidSet();
        }
        if (indexWriter != null) {
            return indexWriter.getGtidSet();
        }
        if (continueGtidSetSnapshot != null) {
            return continueGtidSetSnapshot;
        }
        throw new IllegalStateException(
                "index writers unbound and continueGtidSet snapshot missing; refuse startGtidSet");
    }

    /**
     * After rotate-failure unbind: bind writers to the current tip using the snapshot continueGtidSet.
     * Already bound → no-op. Failures propagate (T-H3.CP3).
     */
    @Override
    public synchronized void rebindWritersToCurrentTipIfUnbound() throws IOException {
        makeSureOpen();
        if (indexWriterV2 != null || indexWriter != null) {
            return;
        }
        logger.info("[rebindWritersToCurrentTipIfUnbound] writers unbound, rebind current tip, replId={} segment={}",
                replId, asyncCommandStore.getCurrentSegmentStartOffset());
        doSwitchCmdFile();
    }

    @Override
    public synchronized void write(ByteBuf byteBuf) throws IOException {
        makeSureOpen();
        if (indexWriterV2 == null && indexWriter == null) {
            throw new IllegalStateException("index writer not open");
        }
        streamCommandReader.doRead(byteBuf);
    }

    @Override
    public synchronized void doRotate() throws IOException {
        makeSureOpen();
        this.switchCmdFile(commandWriterCallback.getCommandWriter());
    }

    /**
     * Same IndexStore monitor as locate: flush → cmd roll → rebind writers.
     * Closes the half-rotate window where tip index is empty and seek cannot changeToPre.
     * <p>
     * T-H2.G1 / I2: after {@code cmdRoll} succeeds, FS has advanced the write tip and closed
     * old index handles. If {@link #doSwitchCmdFile()} fails, retry it once (still holding
     * Writer refs so {@link #resolveContinueGtidSet()} works); on final failure unbind and
     * rethrow (no swallow).
     */
    @Override
    public synchronized void rotateWithCmdRoll(IOSupplier<?> cmdRoll) throws IOException {
        makeSureOpen();
        flushWriter();
        cmdRoll.get();
        try {
            doSwitchCmdFile();
        } catch (Exception first) {
            // cmd tip already advanced; retry switch once before clearing closed Writer refs
            logger.warn("[rotateWithCmdRoll][switch failed after cmdRoll, retry doSwitchCmdFile once]{} replId={} segment={}",
                    this, replId, asyncCommandStore.getCurrentSegmentStartOffset(), first);
            try {
                doSwitchCmdFile();
                logger.info("[rotateWithCmdRoll][retry doSwitchCmdFile ok]{} replId={} segment={}",
                        this, replId, asyncCommandStore.getCurrentSegmentStartOffset());
            } catch (Exception retry) {
                unbindIndexWritersAfterRotateFailure();
                logger.error("[rotateWithCmdRoll][retry doSwitchCmdFile failed, writers unbound]{} replId={} segment={}",
                        this, replId, asyncCommandStore.getCurrentSegmentStartOffset(), retry);
                rethrowRotateFailure(retry, first);
            }
        }
    }

    /**
     * Drop Writer refs that may hold {@link AsyncFile} handles already closed by {@code fs.roll}.
     * Must not flush — flush would hit {@code ClosedChannelException} (T-H2.G1 / I2).
     */
    private void unbindIndexWritersAfterRotateFailure() {
        GtidSet live = resolveContinueGtidSet();
        this.continueGtidSetSnapshot = live.clone();
        logger.info("[unbindIndexWritersAfterRotateFailure] snapshot continueGtidSet={} replId={}",
                continueGtidSetSnapshot, replId);
        this.indexWriter = null;
        this.indexWriterV2 = null;
    }

    private static void rethrowRotateFailure(Exception failure, Exception first) throws IOException {
        if (first != null && first != failure) {
            failure.addSuppressed(first);
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw new IOException(failure);
    }

    @Override
    public boolean needRotate() {
        if (streamCommandReader != null && streamCommandReader.isTransactionActive()) {
            logger.debug("[rotateFileIfNecessary] transaction active (size: {}), defer rotation",
                    streamCommandReader.getTransactionSize());
            return false;
        }
        return true;
    }

    @Override
    public synchronized Pair<Long, GtidSet> locateTailOfCmd() {
        return new Pair<>(commandWriterCallback.getCommandWriter().totalLength(), this.getIndexGtidSet());
    }

    @Override
    public boolean preAppend(String uuid, long gno) throws IOException {
        if (gtidCmdFilter.gtidSetContains(uuid, gno)) {
            logger.info("[onCommand] gtid command uuid {},gno {} in lost, ignored", uuid, gno);
            return false;
        }
        return true;
    }

    @Override
    public int postAppend(String uuid, long gno, long offset, ByteBuf commandBuf, RedisOpItem redisOpItem)
            throws IOException {
        int cmdLength = commandBuf.readableBytes();
        int written = appendCmdBuf(commandBuf);
        appendIndex(uuid, gno, offset, List.of(cmdLength));
        if (redisOpItem != null && !isPingOrSelectCmd(redisOpItem)) {
            sendPayloadsToCk(List.of(redisOpItem));
        }
        return written;
    }

    private boolean isPingOrSelectCmd(RedisOpItem redisOpItem) {
        if (redisOpItem.getRedisOpType() == null) {
            return false;
        }
        RedisOpType type = redisOpItem.getRedisOpType();
        return type == RedisOpType.PING || type == RedisOpType.SELECT;
    }

    @Override
    public int batchPostAppend(String uuid, long gno, long offset, List<ByteBuf> commandBufs, List<RedisOpItem> payloads)
            throws IOException {
        List<Integer> cmdLengths = new ArrayList<>(commandBufs.size());
        int written = 0;
        for (ByteBuf buf : commandBufs) {
            if (buf != null) {
                cmdLengths.add(buf.readableBytes());
                written += appendCmdBuf(buf);
            }
        }
        appendIndex(uuid, gno, offset, cmdLengths);
        sendPayloadsToCk(payloads);
        return written;
    }

    private void appendIndex(String uuid, long gno, long offset, List<Integer> cmdLengths) throws IOException {
        if (gno > 0) {
            if (keeperConfig.dualWrite() && indexWriter != null) {
                indexWriter.append(uuid, gno, (int) offset);
            }
            if (indexWriterV2 != null) {
                indexWriterV2.appendGtid(uuid, gno, offset, cmdLengths);
            }
        } else if (indexWriterV2 != null) {
            indexWriterV2.appendNonGtid(offset, cmdLengths);
        }
    }

    @Override
    public boolean checkOffset(long offset) {
        long cmdFileLen = getCurrentCmdFileLen();
        int pendingSize = getPendingSize();
        long logicOffset = cmdFileLen + pendingSize;
        if (-1 != logicOffset && logicOffset != offset) {
            logger.info("[checkOffset][mismatch] nextCmdBegin:{} cmdFileLen{},pendingSize {}", offset, cmdFileLen,
                    pendingSize);
            return false;
        }
        return true;
    }

    @Override
    public RedisOpParser getOpParser() {
        return this.opParser;
    }

    public int appendCmdBuf(ByteBuf byteBuf) throws IOException {
        if (writerCmdEnabled && commandWriterCallback != null) {
            return commandWriterCallback.writeCommand(byteBuf);
        }
        return byteBuf.readableBytes();
    }

    private void sendPayloadsToCk(List<RedisOpItem> payloads) {
        if (ckStore != null && !ckStore.isKeeper()) {
            try {
                ckStore.sendPayloads(payloads);
            } catch (Throwable t) {
                logger.warn("[sendPayloadsToCk][fail]", t);
            }
        }
    }

    /**
     * Short critical section (spec §3.7.10): flush → create+init tip IndexReader → sample Fallback tail.
     * Mutual exclusion with {@link #rotateWithCmdRoll} ensures init never opens a half-rotate tip
     * (empty header → miss → wrong Fallback). Caller must {@link LocateSnapshot#close()}.
     */
    synchronized LocateSnapshot publishLocateSnapshot() throws IOException {
        flushWriter();
        long tipHint = asyncCommandStore.getCurrentSegmentStartOffset();
        long tailBacklogOffset = commandWriterCallback.getCommandWriter().totalLength();
        GtidSet tailGtidSet = getIndexGtidSet();
        long cmdStart = getCmdStoreStartOffset();

        IndexReader reader = createIndexReader(tipHint);
        long tipSegmentStart = tipHint;
        if (reader != null) {
            reader.setCmdStoreStartOffset(cmdStart);
            try {
                reader.init();
                tipSegmentStart = reader.getStartOffset();
            } catch (IOException e) {
                reader.close();
                throw e;
            }
        }
        return new LocateSnapshot(reader, tipSegmentStart, tailBacklogOffset, tailGtidSet, cmdStart);
    }

    @Override
    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException {
        LocateSnapshot snap = publishLocateSnapshot();
        try {
            return seekContinueWithSnapshot(request, snap);
        } finally {
            snap.close();
        }
    }

    private Pair<Long, GtidSet> seekContinueWithSnapshot(GtidSet request, LocateSnapshot snap) throws IOException {
        Runnable hook = afterLocateSnapshotHook;
        if (hook != null) {
            hook.run();
        }
        if (snap.reader == null) {
            logger.info("[locateContinueGtidSet] index reader is null");
            return new Pair<>(-1L, new GtidSet(GtidSet.EMPTY_GTIDSET));
        }
        return snap.reader.seek(request);
    }

    private IndexReader createIndexReader(long tipSegmentStart) throws IOException {
        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        if (keeperConfig.readV2()) {
            return new IndexReaderV2(fs, baseDir, prefix, tipSegmentStart, replId);
        }
        return new IndexReader(fs, baseDir, prefix, tipSegmentStart, replId);
    }

    @Override
    public Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException {
        LocateSnapshot snap = publishLocateSnapshot();
        try {
            Pair<Long, GtidSet> continuePoint = seekContinueWithSnapshot(request, snap);
            if (continuePoint.getKey() == -1) {
                logger.info("[locateGtidSetWithFallbackToEnd] not found next, return snapshot tail, request:{}", request);
                continuePoint = new Pair<>(snap.tailBacklogOffset, snap.tailGtidSet);
            }
            logger.info("[locateGtidSetWithFallbackToEnd] backlogOffset={}, backlog gtid set: {}, request gtid set {}, continue gtid set {}",
                    continuePoint.getKey(), getIndexGtidSet(), request, continuePoint.getValue());
            return continuePoint;
        } finally {
            snap.close();
        }
    }

    /** Snapshot — never return the live writer-owned GtidSet (CME under concurrent write + XSYNC wait). */
    @Override
    public synchronized GtidSet getIndexGtidSet() {
        GtidSet live;
        if (indexWriterV2 != null && keeperConfig.readV2()) {
            live = indexWriterV2.getGtidSet();
        } else if (indexWriter != null) {
            live = indexWriter.getGtidSet();
        } else {
            live = getIndexGtidSetByIndexReader();
        }
        return live == null ? new GtidSet(GtidSet.EMPTY_GTIDSET) : live.clone();
    }

    @Override
    public synchronized boolean increaseLost(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException {
        GtidSet backlogGtidSet = getIndexGtidSet();
        GtidSet intersection = backlogGtidSet.retainAll(lost);
        if (intersection.itemCnt() > 0) {
            throw new LostGtidsetBacklogConflictException("increase lost conflict with backlog");
        }
        return supplier.get();
    }

    public void buildIndexFromCmdFile(long cmdFileOffset) throws IOException {
        buildIndexFromCmdFile(cmdFileOffset, null, null, -1, -1);
    }

    public void buildIndexFromCmdFile(long cmdFileOffset, String indexPrefix, String blockPrefix,
                                      long indexSize, long blockSize) throws IOException {
        if (indexPrefix != null && blockPrefix != null && indexSize >= 0 && blockSize >= 0) {
            truncateIndexFilesAt(indexPrefix, blockPrefix, indexSize, blockSize);
            if (indexSize == 0) {
                ensureIndexHeaderAfterTruncate(indexPrefix);
            }
        }

        this.streamCommandReader = new StreamCommandReader(this, cmdFileOffset);
        disableWriterCmd();
        AsyncSegmentFile readSeg = openReadSegment(Collections.emptyList());
        try {
            long globalOffset = asyncCommandStore.getCurrentSegmentStartOffset() + cmdFileOffset;
            AsyncFileSystemHelper.await(() -> fs.position(readSeg, globalOffset), "position read segment for rebuild");
            logger.info("[buildIndexFromCmdFile] segmentOffset {} globalOffset {}", cmdFileOffset, globalOffset);

            int cmdCount = 0;
            while (true) {
                int chunkLen = asyncCommandStore.getAsyncWriteMaxBytes();
                ByteBuf byteBuf = AsyncFileSystemHelper.await(() -> fs.read(readSeg, chunkLen), "read cmd for rebuild");
                if (byteBuf == null || !byteBuf.isReadable()) {
                    if (byteBuf != null) {
                        byteBuf.release();
                    }
                    break;
                }
                int readLen = byteBuf.readableBytes();
                try {
                    this.write(byteBuf);
                    cmdCount++;
                } catch (Exception e) {
                    logger.error("[buildIndexFromCmdFile] cmdCount {}", cmdCount, e);
                    throw e;
                } finally {
                    byteBuf.release();
                }
                if (readLen < chunkLen) {
                    break;
                }
            }

            if (indexPrefix != null && blockPrefix != null) {
                if (this.streamCommandReader.isTransactionActive()) {
                    long transactionStartOffset = this.streamCommandReader.getTransactionStartOffset();
                    if (transactionStartOffset >= 0) {
                        logger.warn("[buildIndexFromCmdFile] incomplete transaction detected (size: {}), rollback to {}",
                                this.streamCommandReader.getTransactionSize(), transactionStartOffset);
                        EventMonitor.DEFAULT.logAlertEvent("INCOMPLETE_TRANSACTION");
                        asyncCommandStore.truncateCmdSegment(transactionStartOffset);
                        this.streamCommandReader.resetParser();
                    } else {
                        this.streamCommandReader.resetParser();
                    }
                } else if (this.streamCommandReader.getRemainLength() > 0) {
                    EventMonitor.DEFAULT.logAlertEvent("TRUNCATE_CMD_FILE");
                    long truncateOffset = asyncCommandStore.currentSegmentSize()
                            - this.streamCommandReader.getRemainLength();
                    asyncCommandStore.truncateCmdSegment(truncateOffset);
                    this.streamCommandReader.resetParser();
                }
            }
        } finally {
            enableWriterCmd();
            AsyncFileSystemHelper.closeReadHandle(fs, readSeg, "close read segment for rebuild");
        }
    }

    private void truncateIndexFilesAt(String indexPrefix, String blockPrefix, long indexSize, long blockSize)
            throws IOException {
        asyncCommandStore.truncateIndex(indexPrefix, blockPrefix, indexSize, blockSize);
        refreshWriteIndexHandles(getWriteIndexHandles(keeperConfig.dualWrite()));
    }

    /**
     * After truncating index/block to 0, the header is wiped — write it again before scanning cmd bytes.
     * {@link #refreshWriteIndexHandles} already binds write handles and may write header via {@code init};
     * this call makes the contract explicit for the full-rebuild path ({@code indexSize == 0}).
     */
    private void ensureIndexHeaderAfterTruncate(String indexPrefix) throws IOException {
        String cmdPrefix = asyncCommandStore.getCommandFileNamePrefix();
        if (indexPrefix.equals(INDEX_V2 + cmdPrefix)) {
            if (indexWriterV2 != null) {
                indexWriterV2.ensureHeaderIfEmpty();
            }
        } else if (indexPrefix.equals(INDEX + cmdPrefix)) {
            if (indexWriter != null) {
                indexWriter.ensureHeaderIfEmpty();
            }
        }
    }

    private void refreshWriteIndexHandles(Map<String, AsyncFile> handles) throws IOException {
        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        if (indexWriterV2 != null) {
            indexWriterV2.init(handles.get(INDEX_V2 + prefix), handles.get(BLOCK_V2 + prefix));
        }
        if (indexWriter != null && keeperConfig.dualWrite()) {
            indexWriter.init(handles.get(INDEX + prefix), handles.get(BLOCK + prefix));
        }
    }

    AsyncSegmentFile openReadSegment(List<String> indexPrefixes) throws IOException {
        return AsyncFileSystemHelper.awaitOpen(fs, () -> fs.open(baseDir, asyncCommandStore.getCommandFileNamePrefix(), indexPrefixes, false,
                        replId.toString()),
                "open read segment for index rebuild");
    }

    private synchronized GtidSet saveIndex() {
        GtidSet result = null;
        if (keeperConfig.dualWrite() && indexWriter != null) {
            try {
                this.indexWriter.saveIndexEntry();
            } catch (IOException e) {
                logger.error("[locateGtidRange] failed to save index entry", e);
            }
            result = indexWriter.getGtidSet();
        }
        if (indexWriterV2 != null) {
            try {
                this.indexWriterV2.flush();
            } catch (IOException e) {
                logger.error("[locateGtidRange] failed to save index entry", e);
            }
            if (result == null) {
                result = indexWriterV2.getGtidSet();
            }
        }
        return result;
    }

    @Override
    public List<Pair<Long, Long>> locateGtidRange(String uuid, long begGno, long endGno) throws IOException {
        List<Pair<Long, Long>> result = new ArrayList<>();
        GtidSet currentGtidSet = saveIndex();

        GtidSet reqGtidSet = new GtidSet("");
        reqGtidSet.compensate(uuid, begGno, endGno);
        if (null == currentGtidSet || currentGtidSet.retainAll(reqGtidSet).isEmpty()) {
            return result;
        }

        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        IndexReader indexReader = keeperConfig.readV2()
                ? IndexReaderV2.getFirstIndexReader(fs, baseDir, prefix, replId)
                : IndexReader.getFirstIndexReader(fs, baseDir, prefix, replId);
        IndexReader nextIndexReader = null;
        if (indexReader == null) {
            logger.info("[locateGtidRange] index reader is null, uuid: {}, begGno: {}, endGno: {}", uuid, begGno,
                    endGno);
            return result;
        }

        try {
            indexReader.init();
            Long nextOffset = indexReader.findNextSegmentOffset();
            if (nextOffset != null) {
                if (keeperConfig.readV2()) {
                    nextIndexReader = new IndexReaderV2(fs, baseDir, prefix, nextOffset, replId);
                } else {
                    nextIndexReader = new IndexReader(fs, baseDir, prefix, nextOffset, replId);
                }
                nextIndexReader.init();
            }

            boolean changeFileSuccess = true;
            while (changeFileSuccess) {
                if (!indexReader.noIndex()) {
                    try {
                        GtidSet currentIndexGtidSet = null;
                        if (null != nextIndexReader) {
                            currentIndexGtidSet = nextIndexReader.getStartGtidSet()
                                    .subtract(indexReader.getStartGtidSet());
                        }
                        if (null == currentIndexGtidSet || !currentIndexGtidSet.retainAll(reqGtidSet).isEmpty()) {
                            List<Pair<Long, Long>> ranges = indexReader.findMatchingRanges(uuid, begGno, endGno);
                            for (Pair<Long, Long> range : ranges) {
                                long startBacklogOffset = range.getKey() + indexReader.getStartOffset();
                                Long endBacklogOffset = range.getValue();
                                if (endBacklogOffset != null) {
                                    endBacklogOffset = endBacklogOffset + indexReader.getStartOffset();
                                } else {
                                    endBacklogOffset = getSegmentEndBacklogOffset(indexReader.getStartOffset());
                                    if (endBacklogOffset == null) {
                                        continue;
                                    }
                                }
                                result.add(new Pair<>(startBacklogOffset, endBacklogOffset));
                            }
                        }
                    } catch (IOException e) {
                        logger.debug("[locateGtidRange] error searching in current index file", e);
                    }
                }
                try {
                    changeFileSuccess = indexReader.changeToNext();
                    if (changeFileSuccess && nextIndexReader != null) {
                        if (!nextIndexReader.changeToNext()) {
                            nextIndexReader.close();
                            nextIndexReader = null;
                        }
                    }
                } catch (IOException e) {
                    logger.error("[locateGtidRange] failed to change to next index file", e);
                    changeFileSuccess = false;
                }
            }
            return result;
        } finally {
            indexReader.close();
            if (nextIndexReader != null) {
                nextIndexReader.close();
            }
        }
    }

    private Long getSegmentEndBacklogOffset(long segmentStart) {
        try {
            long size = AsyncFileSystemHelper.await(() -> fs.sizeOfSegment(asyncCommandStore.getWriteSegmentFile(), segmentStart),
                    "size cmd segment for end offset");
            return segmentStart + size;
        } catch (IOException e) {
            logger.warn("[locateGtidRange] cannot determine end offset for segment: {}", segmentStart, e);
            return null;
        }
    }

    @Override
    public synchronized void flushWriter() throws IOException {
        makeSureOpen();
        if (this.indexWriter != null) {
            this.indexWriter.flush();
        }
        if (this.indexWriterV2 != null) {
            this.indexWriterV2.flush();
        }
    }

    @Override
    public synchronized void closeWriter() throws IOException {
        // Protocol switch: flush + reset parser; IndexStore remains open for locate / reopen.
        if (this.streamCommandReader != null) {
            this.streamCommandReader.resetParser();
        }
        flushWriter();
    }

    /**
     * Terminal close for store teardown. Index {@code AsyncFile} handles are segment-owned
     * (spec §3.7.1); this only best-effort flushes, drops local refs, and marks closed.
     * Flush failure must not abort close so {@code CmdStore} can still {@code fs.close(seg)}.
     */
    @Override
    public synchronized void close() throws IOException {
        if (!cmpAndSetClosed()) {
            logger.info("[close][already closed]{}", this);
            return;
        }
        logger.info("[close]{}", this);
        try {
            if (this.streamCommandReader != null) {
                this.streamCommandReader.resetParser();
            }
            flushWriterBestEffortOnClose();
        } finally {
            this.indexWriter = null;
            this.indexWriterV2 = null;
            this.streamCommandReader = null;
        }
    }

    private void flushWriterBestEffortOnClose() {
        if (this.indexWriter != null) {
            try {
                this.indexWriter.flush();
            } catch (Throwable t) {
                logger.error("[close][flush indexWriter failed]{}", this, t);
            }
        }
        if (this.indexWriterV2 != null) {
            try {
                this.indexWriterV2.flush();
            } catch (Throwable t) {
                logger.error("[close][flush indexWriterV2 failed]{}", this, t);
            }
        }
    }

    @Override
    public void resetParserState() {
        makeSureOpen();
        if (streamCommandReader != null) {
            streamCommandReader.resetParser();
        }
    }

    public long getCurrentCmdFileLen() {
        if (commandWriterCallback != null) {
            return commandWriterCallback.getCmdFileLen();
        }
        return -1L;
    }

    public int getPendingSize() {
        if (commandWriterCallback != null) {
            return commandWriterCallback.getPendingSize();
        }
        return 0;
    }

    public IndexWriterV2 getIndexWriterV2() {
        return indexWriterV2;
    }
    public IndexWriter getIndexWriterV1() {
        return indexWriter;
    }

    private void disableWriterCmd() {
        this.writerCmdEnabled = false;
    }

    private void enableWriterCmd() {
        this.writerCmdEnabled = true;
    }

    private GtidSet getIndexGtidSetByIndexReader() {
        try {
            return tryGetIndexGtidSet();
        } catch (IOException ioException) {
            logger.error("[getIndexGtidSetByIndexReader] {}", ioException);
            throw new XpipeRuntimeException("index reader error", ioException);
        }
    }

    private GtidSet tryGetIndexGtidSet() throws IOException {
        IndexReader indexReader = createIndexReader(asyncCommandStore.getCurrentSegmentStartOffset());
        try {
            indexReader.init();
            return indexReader.getAllGtidSet();
        } finally {
            indexReader.close();
        }
    }
}
