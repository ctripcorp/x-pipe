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

public class DefaultIndexStore implements IndexStore, StreamTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(DefaultIndexStore.class);

    private IndexWriter indexWriter;

    private NonGtidIndexWriter nonGtidIndexWriter;

    private StreamCommandReader streamCommandReader;

    private String baseDir;

    private String currentCmdFileName;

    private RedisOpParser opParser;

    private GtidSet startGtidSet;

    private CommandWriterCallback commandWriterCallback;

    private GtidCmdFilter gtidCmdFilter;

    private boolean writerCmdEnabled;

    private CKStore ckStore;

    public DefaultIndexStore(String baseDir, RedisOpParser redisOpParser,
                             CommandWriterCallback commandWriterCallback, GtidCmdFilter gtidCmdFilter, String currentCmdFileName) {
        this.baseDir = baseDir;
        this.opParser = redisOpParser;
        this.commandWriterCallback = commandWriterCallback;
        this.startGtidSet = new GtidSet("");
        this.gtidCmdFilter = gtidCmdFilter;
        this.writerCmdEnabled = true;
        this.currentCmdFileName = currentCmdFileName;
    }

    public DefaultIndexStore(CKStore ckStore, String baseDir, RedisOpParser redisOpParser,
                             CommandWriterCallback commandWriterCallback, GtidCmdFilter gtidCmdFilter, String currentCmdFileName) {
        this(baseDir,redisOpParser,commandWriterCallback,gtidCmdFilter,currentCmdFileName);
        this.ckStore = ckStore;
    }

    @Override
    public void openWriter(CommandWriter cmdWriter) throws IOException {
        this.currentCmdFileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        this.streamCommandReader = new StreamCommandReader(this, cmdWriter.getFileContext().getChannel().size());
        this.nonGtidIndexWriter = new NonGtidIndexWriter(baseDir, currentCmdFileName);
        this.nonGtidIndexWriter.init();
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, startGtidSet, this);
        this.indexWriter.init();
    }

    @Override
    public synchronized void write(ByteBuf byteBuf) throws IOException {
        if(indexWriter == null) {
            throw new IllegalStateException("index writer not open");
        }
        streamCommandReader.doRead(byteBuf);
    }

    public void switchCmdFile(CommandWriter cmdWriter) throws IOException {
        String fileName = cmdWriter.getFileContext().getCommandFile().getFile().getName();
        doSwitchCmdFile(fileName);
    }

    public synchronized void doSwitchCmdFile(String cmdFileName) throws IOException {
        GtidSet continueGtidSet = this.indexWriter.getGtidSet();
        this.currentCmdFileName = cmdFileName;
        this.indexWriter.close();
        if (this.nonGtidIndexWriter != null) {
            this.nonGtidIndexWriter.close();
        }
        this.nonGtidIndexWriter = new NonGtidIndexWriter(baseDir, currentCmdFileName);
        this.nonGtidIndexWriter.init();
        this.indexWriter = new IndexWriter(baseDir, currentCmdFileName, continueGtidSet, this);
        this.indexWriter.init();
        this.streamCommandReader.resetOffset();
        logger.info("[switchCmdFile] index_store switch to {}", currentCmdFileName);
    }

    @Override
    public synchronized void rotateFileIfNecessary() throws IOException {
        if (streamCommandReader != null && streamCommandReader.isTransactionActive()) {
            logger.debug("[rotateFileIfNecessary] transaction active (size: {}), defer rotation",
                      streamCommandReader.getTransactionSize());
            return;
        }

        boolean rotate = commandWriterCallback.getCommandWriter().rotateFileIfNecessary();
        if(rotate) {
            this.switchCmdFile(commandWriterCallback.getCommandWriter());
        }
    }

    @Override
    public synchronized Pair<Long, GtidSet> locateTailOfCmd() {
        return new Pair<>(commandWriterCallback.getCommandWriter().totalLength(), this.getIndexGtidSet());
    }

    @Override
    public boolean preAppend(String gtid, long offset) throws IOException {
//        String[] parts = gtid.split(":");
//        if (parts.length != 2 || parts[0].length() != 40) {
//            throw new IllegalArgumentException("Invalid gtid: " + gtid);
//        }
        String uuid = gtid.substring(0,40);
        long gno = Long.parseLong(gtid.substring(41));
        if(gtidCmdFilter.gtidSetContains(uuid, gno)) {
            logger.info("[onCommand] gtid command {} in lost, ignored", gtid);
            return false;
        }
        indexWriter.append(uuid, gno, (int)offset);
        return true;
    }

    @Override
    public int postAppend(ByteBuf commandBuf, RedisOpItem redisOpItem) throws IOException {
        int written = appendCmdBuf(commandBuf);
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
    public int batchPostAppend(List<ByteBuf> commandBufs, List<RedisOpItem> payloads) throws IOException {
        int written = 0;
        for (ByteBuf buf : commandBufs) {
            if (buf != null) {
                written += appendCmdBuf(buf);
            }
        }
        sendPayloadsToCk(payloads);
        return written;
    }

    @Override
    public boolean checkOffset(long offset) {
        long cmdFileLen = getCurrentCmdFileLen();
        if (-1 != cmdFileLen && cmdFileLen != offset) {
            logger.info("[checkOffset][mismatch] nextCmdBegin:{} cmdFileLen{}", offset, cmdFileLen);
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
        return 0;
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
        if(indexWriter != null) {
            this.indexWriter.saveIndexEntry();
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
        if(indexWriter != null) {
            return new IndexReader(baseDir, currentCmdFileName);
        } else {
            return IndexReader.getLastIndexReader(baseDir);
        }
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
        if(indexWriter == null) {
            // search from reader
            return getIndexGtidSetByIndexReader();
        }
        return indexWriter.getGtidSet();
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
        long fileSize = new File(Paths.get(baseDir, cmdFileName).toString()).length();
        buildIndexFromCmdFile(cmdFileName, cmdFileOffset, fileSize, true);
    }

    /**
     * 重建 cmd 文件中 [startOffset, endOffset) 区间的 GTID 索引。
     * isLastSegment 为 true 时才允许在尾部对 cmd 文件做 truncate（用于不完整 tx / 残包）。
     */
    public void buildIndexFromCmdFile(String cmdFileName, long startOffset, long endOffset, boolean isLastSegment) throws IOException {
        if (startOffset >= endOffset) return;
        this.streamCommandReader = new StreamCommandReader(this, startOffset);
        this.disableWriterCmd();
        ControllableFile controllableFile = null;
        try {
            controllableFile = new DefaultControllableFile(new File(Paths.get(baseDir, cmdFileName).toString()));
            controllableFile.getFileChannel().position(startOffset);
            long pos = startOffset;
            while (pos < endOffset) {
                int size = (int) Math.min(1024 * 8, endOffset - pos);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                int n = controllableFile.getFileChannel().read(buffer);
                if (n <= 0) break;
                pos += n;
                buffer.flip();
                ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer.array(), 0, n);
                this.write(byteBuf);
            }

            if (!isLastSegment) {
                // 中间段：边界在 zone.start（命令边界）上，理论上 parser 应当干净。
                if (streamCommandReader.isTransactionActive() || streamCommandReader.getRemainLength() > 0) {
                    logger.warn("[buildIndexFromCmdFile] unclean parser at segment end [{}, {}), txActive={}, remain={}",
                            startOffset, endOffset, streamCommandReader.isTransactionActive(),
                            streamCommandReader.getRemainLength());
                }
                this.streamCommandReader.resetParser();
                return;
            }

            // Check for incomplete protocol parsing
            int remainBytes = this.streamCommandReader.getRemainLength();
            if (this.streamCommandReader.isTransactionActive()) {
                long transactionStartOffset = this.streamCommandReader.getTransactionStartOffset();
                if (transactionStartOffset >= 0) {
                    logger.warn("[buildIndexFromCmdFile] incomplete transaction detected (size: {}), " +
                            "rollback from offset {} to offset: {}",
                            this.streamCommandReader.getTransactionSize(),
                            controllableFile.size(), transactionStartOffset);
                    EventMonitor.DEFAULT.logAlertEvent("INCOMPLETE_TRANSACTION");
                    controllableFile.setLength((int) transactionStartOffset);
                    this.streamCommandReader.resetParser();
                } else {
                    logger.warn("[buildIndexFromCmdFile] incomplete transaction detected but invalid startOffset, " +
                            "clearing transaction state");
                    this.streamCommandReader.resetParser();
                }
            } else if (remainBytes > 0) {
                EventMonitor.DEFAULT.logAlertEvent("TRUNCATE_CMD_FILE");
                controllableFile.setLength((int) controllableFile.size() - remainBytes);
                this.streamCommandReader.resetParser();
            }

        } finally {
            // 从cmd 读 写完之后再加入写
            this.enableWriterCmd();
            if (controllableFile != null) {
                controllableFile.close();
            }
        }
    }

    /**
     * 按 zone 分段重建：跳过非 GTID 区间，只对 GTID 区间走 legacy。
     * 末尾未刷盘的尾部（≤ FLUSH_THRESHOLD 条非 GTID 命令）自然落在最后一段，跟着 legacy 一起处理。
     */
    public void buildIndexFromCmdFileWithZones(String cmdFileName, long startOffset) throws IOException {
        File cmdFile = new File(Paths.get(baseDir, cmdFileName).toString());
        long cmdFileSize = cmdFile.exists() ? cmdFile.length() : 0;
        if (startOffset >= cmdFileSize) return;

        List<long[]> zones = (nonGtidIndexWriter != null)
                ? nonGtidIndexWriter.loadAllZones()
                : java.util.Collections.emptyList();

        List<long[]> usable = new ArrayList<>(zones.size());
        for (long[] z : zones) {
            if (z[1] <= startOffset) continue;
            if (z[1] > cmdFileSize) {
                logger.warn("[buildIndexFromCmdFileWithZones] drop zone [{}, {}) beyond cmdSize {}",
                        z[0], z[1], cmdFileSize);
                continue;
            }
            long s = Math.max(z[0], startOffset);
            if (s >= z[1]) continue;
            usable.add(new long[]{s, z[1]});
        }

        long current = startOffset;
        for (long[] z : usable) {
            if (z[0] > current) {
                buildIndexFromCmdFile(cmdFileName, current, z[0], false);
            }
            current = z[1];
        }
        if (current < cmdFileSize) {
            buildIndexFromCmdFile(cmdFileName, current, cmdFileSize, true);
        }
        logger.info("[buildIndexFromCmdFileWithZones] {}: zones used={}, startOffset={}, cmdSize={}",
                cmdFileName, usable.size(), startOffset, cmdFileSize);
    }

    @Override
    public void onNonGtidWritten(long offset, int length) throws IOException {
        if (nonGtidIndexWriter != null) {
            nonGtidIndexWriter.appendNonGtid(offset, length);
        }
    }

    @Override
    public void onGtidWritten(long offset, int length) throws IOException {
        if (nonGtidIndexWriter != null) {
            nonGtidIndexWriter.onGtid();
        }
    }

    private synchronized GtidSet saveIndex() {
        if (indexWriter != null) {
            try {
                this.indexWriter.saveIndexEntry();
            } catch (IOException e) {
                logger.error("[locateGtidRange] failed to save index entry", e);
            }
            return indexWriter.getGtidSet();
        }
        return null;
    }

    @Override
    public List<Pair<Long, Long>> locateGtidRange(String uuid, int begGno, int endGno) throws IOException {
        List<Pair<Long, Long>> result = new ArrayList<>();
        GtidSet currentGtidSet = saveIndex();

        GtidSet reqGtidSet = new GtidSet("");
        reqGtidSet.compensate(uuid, begGno, endGno);
        if (null == currentGtidSet || currentGtidSet.retainAll(reqGtidSet).isEmpty()) {
            return result;
        }

        // Start from the first index file since GNO is monotonically increasing
        IndexReader indexReader = IndexReader.getFirstIndexReader(baseDir);
        IndexReader nextIndexReader = null;
        if(indexReader == null) {
            logger.info("[locateGtidRange] index reader is null, uuid: {}, begGno: {}, endGno: {}", uuid, begGno, endGno);
            return result;
        }

        try {
            indexReader.init();
            File nextFile = indexReader.findNextFile();
            if (null != nextFile) {
                nextIndexReader = new IndexReader(baseDir, nextFile.getName().replace(INDEX, ""));
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
        if (this.nonGtidIndexWriter != null) {
            try {
                this.nonGtidIndexWriter.close();
            } finally {
                this.nonGtidIndexWriter = null;
            }
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
        NonGtidIndexWriter.deleteZoneFiles(baseDir);
    }

    public void deleteAllIndexFile() {
        File directory = new File(baseDir);

        logger.info("[deleteAllIndexFile] {}", baseDir);
        if (!directory.exists() || !directory.isDirectory()) {
            return;
        }
        File[] files = directory.listFiles((dir, name) -> name.startsWith(INDEX) || name.startsWith(AbstractIndex.BLOCK));
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
            indexReader = IndexReader.getLastIndexReader(baseDir);
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
