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
import com.ctrip.xpipe.redis.keeper.store.AsyncCommandStore;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.BLOCK_V2;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX_V2;

public class DefaultIndexStore implements IndexStore, StreamTransactionListener {

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
    private final CommandWriterCallback commandWriterCallback;
    private final GtidCmdFilter gtidCmdFilter;
    private boolean writerCmdEnabled;
    private final CKStore ckStore;
    private final KeeperConfig keeperConfig;

    public DefaultIndexStore(KeeperConfig keeperConfig, CKStore ckStore, AsyncCommandStore asyncCommandStore,
                             String baseDir, RedisOpParser redisOpParser,
                             CommandWriterCallback commandWriterCallback, GtidCmdFilter gtidCmdFilter) {
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
    }

    public AsyncCommandStore getAsyncCommandStore() {
        return asyncCommandStore;
    }

    @Override
    public void openWriter(CommandWriter cmdWriter) throws IOException {
        this.streamCommandReader = new StreamCommandReader(this, cmdWriter.fileLength());
        openWritersWithHandles(getWriteIndexHandles(keeperConfig.dualWrite()), startGtidSet);
    }

    private void openWritersWithHandles(Map<String, AsyncFile> writeHandles, GtidSet headerGtidSet) throws IOException {
        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        List<String> prefixes = writerIndexPrefixes(keeperConfig.dualWrite());

        AsyncSegmentFile recoverSeg = openReadSegment(prefixes);
        try {
            long segStart = asyncCommandStore.getCurrentSegmentStartOffset();
            AsyncFileSystemHelper.await(fs.position(recoverSeg, segStart), "position recover segment");
            Map<String, AsyncFile> readHandles = AsyncFileSystemHelper.await(
                    fs.getCurrentIndexFiles(recoverSeg, prefixes), "get read index handles for recover");

            if (keeperConfig.dualWrite()) {
                this.indexWriter = new IndexWriter(headerGtidSet, this);
                AsyncFile readIndex = readHandles.get(INDEX + prefix);
                if (readIndex != null && indexFileNeedsRecover(readIndex)) {
                    indexWriter.recoverIndex(readIndex, readHandles.get(BLOCK + prefix));
                }
            }
            this.indexWriterV2 = new IndexWriterV2(headerGtidSet, this,
                    keeperConfig.getIndexZoneConsecutiveThreshold(),
                    keeperConfig.getIndexMixedTotalBytesThreshold(),
                    keeperConfig.getBlockSizeThreshold());
            AsyncFile readIndexV2 = readHandles.get(INDEX_V2 + prefix);
            if (readIndexV2 != null && indexFileNeedsRecover(readIndexV2)) {
                indexWriterV2.recoverIndex(readIndexV2, readHandles.get(BLOCK_V2 + prefix));
            }
        } finally {
            AsyncFileSystemHelper.await(fs.close(recoverSeg), "close recover segment");
        }

        if (keeperConfig.dualWrite()) {
            indexWriter.init(writeHandles.get(INDEX + prefix), writeHandles.get(BLOCK + prefix));
        }
        indexWriterV2.init(writeHandles.get(INDEX_V2 + prefix), writeHandles.get(BLOCK_V2 + prefix));
    }

    private boolean indexFileNeedsRecover(AsyncFile readIndexFile) throws IOException {
        return AsyncFileSystemHelper.await(fs.size(readIndexFile), "size index for recover check") > 0;
    }

    private Map<String, AsyncFile> getWriteIndexHandles(boolean dualWrite) throws IOException {
        List<String> prefixes = writerIndexPrefixes(dualWrite);
        return AsyncFileSystemHelper.await(
                fs.getCurrentIndexFiles(asyncCommandStore.getWriteSegmentFile(), prefixes),
                "get write index handles");
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

    public synchronized void doSwitchCmdFile() throws IOException {
        GtidSet continueGtidSet = resolveContinueGtidSet();
        if (indexWriter != null) {
            indexWriter.close();
        }
        if (indexWriterV2 != null) {
            indexWriterV2.close();
        }
        openWritersWithHandles(getWriteIndexHandles(keeperConfig.dualWrite()), continueGtidSet);
        this.streamCommandReader.resetOffset();
        logger.info("[switchCmdFile] index_store switch to segment {}", asyncCommandStore.getCurrentSegmentStartOffset());
    }

    private GtidSet resolveContinueGtidSet() {
        if (indexWriterV2 != null) {
            return indexWriterV2.getGtidSet();
        }
        if (indexWriter != null) {
            return indexWriter.getGtidSet();
        }
        return startGtidSet;
    }

    @Override
    public synchronized void write(ByteBuf byteBuf) throws IOException {
        if (indexWriterV2 == null && indexWriter == null) {
            throw new IllegalStateException("index writer not open");
        }
        streamCommandReader.doRead(byteBuf);
    }

    @Override
    public synchronized void doRotate() throws IOException {
        this.switchCmdFile(commandWriterCallback.getCommandWriter());
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

    @Override
    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException {
        if (keeperConfig.dualWrite() && indexWriter != null) {
            this.indexWriter.saveIndexEntry();
        }
        if (indexWriterV2 != null) {
            this.indexWriterV2.flushIndexEntry();
        }
        try (IndexReader indexReader = createIndexReader()) {
            if (indexReader == null) {
                logger.info("[locateContinueGtidSet] index reader is null");
                return new Pair<>(-1L, new GtidSet(GtidSet.EMPTY_GTIDSET));
            }
            indexReader.init();
            return indexReader.seek(request);
        }
    }

    private IndexReader createIndexReader() throws IOException {
        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        long segmentStart = asyncCommandStore.getCurrentSegmentStartOffset();
        if (indexWriterV2 != null && keeperConfig.readV2()) {
            return new IndexReaderV2(fs, baseDir, prefix, segmentStart, replId);
        }
        if (indexWriter != null) {
            return new IndexReader(fs, baseDir, prefix, segmentStart, replId);
        }
        return keeperConfig.readV2()
                ? IndexReaderV2.getLastIndexReader(fs, baseDir, prefix, replId)
                : IndexReader.getLastIndexReader(fs, baseDir, prefix, replId);
    }

    @Override
    public synchronized Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException {
        Pair<Long, GtidSet> continuePoint = locateContinueGtidSet(request);
        if (continuePoint.getKey() == -1) {
            logger.info("[locateGtidSetWithFallbackToEnd] not found next, return tail of cmd, request:{}", request);
            continuePoint = locateTailOfCmd();
        }
        logger.info("backlog gtid set: {}, request gtid set {}, continue gtid set {}", getIndexGtidSet(),
                request, continuePoint.getValue());
        return continuePoint;
    }

    @Override
    public synchronized GtidSet getIndexGtidSet() {
        if (indexWriterV2 != null && keeperConfig.readV2()) {
            return indexWriterV2.getGtidSet();
        }
        if (indexWriter != null) {
            return indexWriter.getGtidSet();
        }
        return getIndexGtidSetByIndexReader();
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
            AsyncFileSystemHelper.await(fs.position(readSeg, globalOffset), "position read segment for rebuild");
            logger.info("[buildIndexFromCmdFile] segmentOffset {} globalOffset {}", cmdFileOffset, globalOffset);

            int cmdCount = 0;
            while (true) {
                int chunkLen = asyncCommandStore.getAsyncWriteMaxBytes();
                ByteBuf byteBuf = AsyncFileSystemHelper.await(fs.read(readSeg, chunkLen), "read cmd for rebuild");
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
            AsyncFileSystemHelper.await(fs.close(readSeg), "close read segment for rebuild");
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
        return AsyncFileSystemHelper.await(
                fs.open(baseDir, asyncCommandStore.getCommandFileNamePrefix(), indexPrefixes, false,
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
                this.indexWriterV2.flushIndexEntry();
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
            long size = AsyncFileSystemHelper.await(
                    fs.sizeOfSegment(asyncCommandStore.getWriteSegmentFile(), segmentStart),
                    "size cmd segment for end offset");
            return segmentStart + size;
        } catch (IOException e) {
            logger.warn("[locateGtidRange] cannot determine end offset for segment: {}", segmentStart, e);
            return null;
        }
    }

    @Override
    public synchronized void closeWriter() throws IOException {
        if (this.streamCommandReader != null) {
            this.streamCommandReader.resetParser();
        }
        if (this.indexWriter != null) {
            this.indexWriter.close();
        }
        if (this.indexWriterV2 != null) {
            this.indexWriterV2.close();
        }
    }

    @Override
    public void resetParserState() {
        if (streamCommandReader != null) {
            streamCommandReader.resetParser();
        }
    }

    @Override
    public void closeWithDeleteIndexFiles() throws IOException {
        this.closeWriter();
        deleteAllIndexFile();
    }

    public void deleteAllIndexFile() throws IOException {
        logger.info("[deleteAllIndexFile] {}", baseDir);
        Map<String, AsyncFile> handles = getWriteIndexHandles(true);
        for (AsyncFile file : handles.values()) {
            if (file != null) {
                AsyncFileSystemHelper.await(fs.truncate(file, 0), "truncate index file on deleteAll");
            }
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
        String prefix = asyncCommandStore.getCommandFileNamePrefix();
        IndexReader indexReader = keeperConfig.readV2()
                ? IndexReaderV2.getLastIndexReader(fs, baseDir, prefix, replId)
                : IndexReader.getLastIndexReader(fs, baseDir, prefix, replId);
        if (indexReader == null) {
            return new GtidSet(GtidSet.EMPTY_GTIDSET);
        }
        try {
            indexReader.init();
            return indexReader.getAllGtidSet();
        } finally {
            indexReader.close();
        }
    }
}
