package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.utils.ControllableFile;
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
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.exception.replication.LostGtidsetBacklogConflictException;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX;
import static com.ctrip.xpipe.redis.keeper.store.gtid.index.AbstractIndex.INDEX_V2;

public class DefaultIndexStore implements IndexStore, StreamTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultIndexStore.class);

    private IndexWriter indexWriter;

    private IndexWriterV2 indexWriterV2;      // v2


    private StreamCommandReader streamCommandReader;

    private String baseDir;

    private String currentCmdFileName;

    private RedisOpParser opParser;

    private GtidSet startGtidSet;

    private CommandWriterCallback commandWriterCallback;

    private GtidCmdFilter gtidCmdFilter;

    private boolean writerCmdEnabled;

    private CKStore ckStore;

    private KeeperConfig keeperConfig;

    public DefaultIndexStore(KeeperConfig keeperConfig, CKStore ckStore, String baseDir, RedisOpParser redisOpParser,
                             CommandWriterCallback commandWriterCallback, GtidCmdFilter gtidCmdFilter, String currentCmdFileName) {
        this.baseDir = baseDir;
        this.opParser = redisOpParser;
        this.commandWriterCallback = commandWriterCallback;
        this.startGtidSet = new GtidSet("");
        this.gtidCmdFilter = gtidCmdFilter;
        this.writerCmdEnabled = true;
        this.currentCmdFileName = currentCmdFileName;
        this.keeperConfig = keeperConfig;
        this.ckStore = ckStore;
    }

    @Override
    public void openWriter(CommandWriter cmdWriter) throws IOException {
        this.currentCmdFileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        this.streamCommandReader = new StreamCommandReader(this, cmdWriter.getFileContext().getChannel().size());
        if(keeperConfig.dualWrite()) {
            this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, startGtidSet, this);
            this.indexWriter.init();
        }
        this.indexWriterV2 = new IndexWriterV2(baseDir, currentCmdFileName, startGtidSet, this,
                keeperConfig.getIndexZoneConsecutiveThreshold(),
                keeperConfig.getIndexMixedTotalBytesThreshold());
        this.indexWriterV2.init();
    }

    @Override
    public synchronized void write(ByteBuf byteBuf) throws IOException {
        if(indexWriterV2 == null && indexWriter == null) {
            throw new IllegalStateException("index writer not open");
        }
        streamCommandReader.doRead(byteBuf);
    }

    public void switchCmdFile(CommandWriter cmdWriter) throws IOException {
        String fileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        doSwitchCmdFile(fileName);
    }

    public synchronized void doSwitchCmdFile(String cmdFileName) throws IOException {
        GtidSet continueGtidSet = (indexWriterV2 != null) ? indexWriterV2.getGtidSet() : indexWriter.getGtidSet();
        if (indexWriter != null) indexWriter.close();
        if (indexWriterV2 != null) indexWriterV2.close();
        if (keeperConfig.dualWrite()) {
            this.indexWriter = new IndexWriter(baseDir, cmdFileName, continueGtidSet, this);
            this.indexWriter.init();
        }
        this.indexWriterV2 = new IndexWriterV2(baseDir, cmdFileName, continueGtidSet, this,
                keeperConfig.getIndexZoneConsecutiveThreshold(),
                keeperConfig.getIndexMixedTotalBytesThreshold());
        this.indexWriterV2.init();
        this.currentCmdFileName = cmdFileName;
        this.streamCommandReader.resetOffset();
        logger.info("[switchCmdFile] index_store switch to {}", currentCmdFileName);
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
    public boolean preAppend(String uuid,long gno) throws IOException {
//        String[] parts = gtid.split(":");
//        if (parts.length != 2 || parts[0].length() != 40) {
//            throw new IllegalArgumentException("Invalid gtid: " + gtid);
//        }

        if(gtidCmdFilter.gtidSetContains(uuid, gno)) {
            logger.info("[onCommand] gtid command uuid {},gno {} in lost, ignored", uuid,gno);
            return false;
        }
        return true;
    }

    @Override
    public int postAppend(String uuid,long gno, long offset, ByteBuf commandBuf, RedisOpItem redisOpItem) throws IOException {
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
    public int batchPostAppend(String uuid,long gno, long offset, List<ByteBuf> commandBufs, List<RedisOpItem> payloads) throws IOException {
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
        } else {
            if (indexWriterV2 != null) {
                indexWriterV2.appendNonGtid(offset, cmdLengths);
            }
        }
    }

    @Override
    public boolean checkOffset(long offset) {
        long cmdFileLen = getCurrentCmdFileLen();
        int pendingSize = getPendingSize();
        long logicOffset = cmdFileLen + pendingSize;
        if (-1 != logicOffset && logicOffset != offset) {
            logger.info("[checkOffset][mismatch] nextCmdBegin:{} cmdFileLen{},pendingSize {}", offset, cmdFileLen,pendingSize);
            return false;
        }
        return true;
    }

    @Override
    public RedisOpParser getOpParser() {
        return this.opParser;
    }

    public int appendCmdBuf(ByteBuf byteBuf) throws IOException {
        if(writerCmdEnabled && commandWriterCallback != null) {
            return commandWriterCallback.writeCommand(byteBuf);
        }
        return byteBuf.readableBytes();
    }

    private void sendPayloadsToCk(List<RedisOpItem> payloads){
        if (ckStore != null && !ckStore.isKeeper()) {
            try {
                ckStore.sendPayloads(payloads);
            }catch (Throwable t) {
                logger.warn("[sendPayloadsToCk][fail]", t);
            }
        }
    }

    @Override
    public Pair<Long, GtidSet> locateContinueGtidSet(GtidSet request) throws IOException {
        if(keeperConfig.dualWrite()) {
            if (indexWriter != null) {
                this.indexWriter.saveIndexEntry();
            }
        }
        if (indexWriterV2 != null) {
            this.indexWriterV2.flushIndexEntry();
        }
        try (IndexReader indexReader = createIndexReader()) {
            if(indexReader == null) {
                // no index file
                logger.info("[locateContinueGtidSet] index reader is null");
                return new Pair<>(-1l, new GtidSet(GtidSet.EMPTY_GTIDSET));
            }
            indexReader.init();
            return indexReader.seek(request);
        }
    }

    private IndexReader createIndexReader() throws IOException {
        if (indexWriterV2 != null && keeperConfig.readV2()) {
            return new IndexReaderV2(baseDir, currentCmdFileName);
        }
        if (indexWriter != null) {
            return new IndexReader(baseDir, currentCmdFileName);
        }
        return keeperConfig.readV2() ? IndexReaderV2.getLastIndexReader(baseDir) : IndexReader.getLastIndexReader(baseDir);
    }

    @Override
    public synchronized Pair<Long, GtidSet> locateGtidSetWithFallbackToEnd(GtidSet request) throws IOException {
        Pair<Long, GtidSet> continuePoint = locateContinueGtidSet(request);
        if(continuePoint.getKey() == -1) {
            logger.info("[locateGtidSetWithFallbackToEnd] not found next, return tail of cmd, request:{}", request);
            continuePoint = locateTailOfCmd();
        }
        logger.info("backlog gtid set: {}, request gtid set {}, continue gtid set {}", getIndexGtidSet(),
                request, continuePoint.getValue());
        return continuePoint;
    }

    @Override
    public synchronized GtidSet getIndexGtidSet() {
        if (indexWriterV2 != null && keeperConfig.readV2()) return indexWriterV2.getGtidSet();
        if (indexWriter != null) return indexWriter.getGtidSet();
        return getIndexGtidSetByIndexReader();
    }

    @Override
    public synchronized boolean increaseLost(GtidSet lost, IOSupplier<Boolean> supplier) throws IOException {
        GtidSet backlogGtidSet = getIndexGtidSet();
        GtidSet intersection = backlogGtidSet.retainAll(lost);
        if(intersection.itemCnt() > 0) {
            throw new LostGtidsetBacklogConflictException("increase lost conflict with backlog");
        }
        return supplier.get();
    }

    public void buildIndexFromCmdFile(String cmdFileName, long cmdFileOffset) throws IOException {
        this.streamCommandReader = new StreamCommandReader(this, cmdFileOffset);
        this.disableWriterCmd();
        ControllableFile controllableFile = null;
        try {
            controllableFile = new DefaultControllableFile(new File(Paths.get(baseDir, cmdFileName).toString()));
            controllableFile.getFileChannel().position(cmdFileOffset);
            logger.info("[buildIndexFromCmdFile] currentOffset {},fileSize {}",cmdFileOffset,controllableFile.size());

            int cmdCount = 0;
            while(controllableFile.getFileChannel().position() < controllableFile.getFileChannel().size()) {
                int size = (int)Math.min(1024*8, controllableFile.getFileChannel().size() - controllableFile.getFileChannel().position());
                ByteBuffer buffer = ByteBuffer.allocate(size);
                int readable = controllableFile.getFileChannel().read(buffer);
                buffer.flip();
                ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array());
                try {
                    this.write(byteBuf);
                    cmdCount++;
                }catch (Exception e){
                    logger.error("[read] content {},pos {},size {},readable {},cmdCount {}",new String(buffer.array()),controllableFile.getFileChannel().position(),size,readable,cmdCount,e);
                    throw e;
                }
            }
            
            // Check for incomplete protocol parsing
            int remainBytes = this.streamCommandReader.getRemainLength();
            if (this.streamCommandReader.isTransactionActive()) {
                // Check for incomplete transaction (MULTI without EXEC)
                // If there's an active transaction, it means the transaction was not committed
                // We need to rollback by truncating the file to the transaction start offset
                long transactionStartOffset = this.streamCommandReader.getTransactionStartOffset();
                if (transactionStartOffset >= 0) {
                    // transactionStartOffset is relative to cmdFileOffset, convert to absolute offset
                    logger.warn("[buildIndexFromCmdFile] incomplete transaction detected (size: {}), " +
                            "rollback from offset {} to offset: {}", 
                            this.streamCommandReader.getTransactionSize(), 
                            controllableFile.size(), transactionStartOffset);
                    EventMonitor.DEFAULT.logAlertEvent("INCOMPLETE_TRANSACTION");
                    // Truncate file to transaction start offset to rollback incomplete transaction
                    controllableFile.setLength((int)transactionStartOffset);
                    commandWriterCallback.getCommandWriter().getFileContext().setFileLength(transactionStartOffset);
                    this.streamCommandReader.resetParser();
                } else {
                    // If startOffset is invalid, just reset parser to clear transaction state
                    logger.warn("[buildIndexFromCmdFile] incomplete transaction detected but invalid startOffset, " +
                            "clearing transaction state");
                    this.streamCommandReader.resetParser();
                }
            } else if (remainBytes > 0) {
                // Check for incomplete protocol parsing
                EventMonitor.DEFAULT.logAlertEvent("TRUNCATE_CMD_FILE");
                controllableFile.setLength((int)controllableFile.size() - remainBytes);
                commandWriterCallback.getCommandWriter().getFileContext().setFileLength(controllableFile.size()-remainBytes);
                this.streamCommandReader.resetParser();
            }

        } finally {
            // 从cmd 读 写完之后再加入写
            this.enableWriterCmd();
            if(controllableFile != null) {
                controllableFile.close();
            }
        }
    }

    private synchronized GtidSet saveIndex() {
        GtidSet result = null;
        if(keeperConfig.dualWrite()) {
            if (indexWriter != null) {
                try {
                    this.indexWriter.saveIndexEntry();
                } catch (IOException e) {
                    logger.error("[locateGtidRange] failed to save index entry", e);
                }
                result = indexWriter.getGtidSet();
            }
        }

        if (indexWriterV2 != null) {
            try {
                this.indexWriterV2.flushIndexEntry();
            } catch (IOException e) {
                logger.error("[locateGtidRange] failed to save index entry", e);
            }
            if(result == null) {
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

        // Start from the first index file since GNO is monotonically increasing
        IndexReader indexReader = null;
        if(keeperConfig.readV2()) {
            indexReader = IndexReaderV2.getFirstIndexReader(baseDir);
        }else {
            indexReader = IndexReader.getFirstIndexReader(baseDir);
        }
        IndexReader nextIndexReader = null;
        if(indexReader == null) {
            logger.info("[locateGtidRange] index reader is null, uuid: {}, begGno: {}, endGno: {}", uuid, begGno, endGno);
            return result;
        }

        try {
            indexReader.init();
            File nextFile = indexReader.findNextFile();
            if (null != nextFile) {
                if(keeperConfig.readV2()) {
                    nextIndexReader = new IndexReaderV2(baseDir, nextFile.getName().replace(INDEX_V2, ""));
                }else {
                    nextIndexReader = new IndexReader(baseDir, nextFile.getName().replace(INDEX, ""));
                }
                nextIndexReader.init();
            }

            // Search through all index files from the first one
            boolean changeFileSuccess = true;
            while (changeFileSuccess) {
                // skip empty index file
                // For example, the cmd file is full of "PUB"
                if (!indexReader.noIndex()) {
                    try {
                        GtidSet currentIndexGtidSet = null;
                        if (null != nextIndexReader) {
                            currentIndexGtidSet = nextIndexReader.getStartGtidSet().subtract(indexReader.getStartGtidSet());
                        }

                        if (null == currentIndexGtidSet || !currentIndexGtidSet.retainAll(reqGtidSet).isEmpty()) {
                            // Find all matching ranges in current index file
                            List<Pair<Long, Long>> ranges = indexReader.findMatchingRanges(uuid, begGno, endGno);

                            // Convert cmdStartOffset to backlogOffset by adding startOffset
                            for (Pair<Long, Long> range : ranges) {
                                long startBacklogOffset = range.getKey() + indexReader.getStartOffset();
                                Long endBacklogOffset = range.getValue();

                                if (endBacklogOffset != null) {
                                    // End offset is the next command's start, convert to backlogOffset
                                    endBacklogOffset = endBacklogOffset + indexReader.getStartOffset();
                                } else {
                                    // End offset is null, meaning it's at file end, use file length
                                    String cmdFileName = indexReader.getFileName();
                                    endBacklogOffset = getFileEndBacklogOffset(cmdFileName);
                                    if (endBacklogOffset == null) {
                                        logger.warn("[locateGtidRange] cannot determine end offset for file: {}", cmdFileName);
                                        continue; // Skip this range if we can't determine end
                                    }
                                }

                                result.add(new Pair<>(startBacklogOffset, endBacklogOffset));
                            }
                        }
                    } catch (IOException e) {
                        logger.debug("[locateGtidRange] error searching in current index file, trying next, uuid: {}, begGno: {}, endGno: {}",
                                uuid, begGno, endGno, e);
                    }
                }
                // Try to find in next index file
                try {
                    changeFileSuccess = indexReader.changeToNext();
                    if(changeFileSuccess) {
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
            
            if(result.isEmpty()) {
                logger.info("[locateGtidRange] GTID not found in range, uuid: {}, begGno: {}, endGno: {}", uuid, begGno, endGno);
            } else {
                logger.debug("[locateGtidRange] found {} ranges, uuid: {}, begGno: {}, endGno: {}",
                        result.size(), uuid, begGno, endGno);
            }
            
            return result;
        } catch (IOException e) {
            logger.error("[locateGtidRange] failed to locate GTID range, uuid: {}, begGno: {}, endGno: {}", uuid, begGno, endGno, e);
            return result;
        } finally {
            if(indexReader != null) {
                indexReader.close();
            }
            if (null != nextIndexReader) {
                nextIndexReader.close();
            }
        }
    }

    /**
     * Get the file end backlogOffset for a given command file name.
     * Returns null if file doesn't exist and totalLength is not available.
     */
    private Long getFileEndBacklogOffset(String cmdFileName) {
        File cmdFile = new File(Paths.get(baseDir, cmdFileName).toString());
        if(cmdFile.exists()) {
            long fileLength = cmdFile.length();
            long cmdFileStartOffset = AbstractIndex.extractOffset(cmdFileName);
            return cmdFileStartOffset + fileLength;
        }
        
        return null;
    }

    @Override
    public synchronized void closeWriter() throws IOException {
        // close = close writer
        if(this.streamCommandReader != null) {
            this.streamCommandReader.resetParser();
        }
        if(this.indexWriter != null) {
            logger.debug("[doClose] close index writer {}", indexWriter.getFileName());
            this.indexWriter.close();
        }
        if (this.indexWriterV2 != null) {
            this.indexWriterV2.close();
        }
    }

    @Override
    public void resetParserState() {
        if(streamCommandReader != null) {
            streamCommandReader.resetParser();
        }
    }

    @Override
    public void closeWithDeleteIndexFiles() throws IOException {
        this.closeWriter();
        deleteAllIndexFile();
    }

    public void deleteAllIndexFile() {
        File directory = new File(baseDir);

        logger.info("[deleteAllIndexFile] {}", baseDir);
        if (!directory.exists() || !directory.isDirectory()) {
            return;
        }
        File[] files = directory.listFiles((dir, name) -> name.startsWith(INDEX) || name.startsWith(AbstractIndex.BLOCK) || name.startsWith(INDEX_V2) || name.startsWith(AbstractIndex.BLOCK_V2));
        if (files == null) {
            return;
        }
        for (File file : files) {
            file.delete();
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
        IndexReader indexReader = null;
        try {
            if(keeperConfig.readV2()) {
                indexReader = IndexReaderV2.getLastIndexReader(baseDir);
            }else {
                indexReader = IndexReader.getLastIndexReader(baseDir);
            }
            if(indexReader == null) {
                return new GtidSet(GtidSet.EMPTY_GTIDSET);
            }
            indexReader.init();
            return indexReader.getAllGtidSet();
        } finally {
            if(indexReader != null) {
                indexReader.close();
            }
        }
    }

}
