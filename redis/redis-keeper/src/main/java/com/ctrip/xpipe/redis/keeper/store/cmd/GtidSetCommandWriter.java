package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.utils.OsUtils.LINE_SEPARATOR;

/**
 * @author lishanglin
 * date 2022/5/20
 */
public class GtidSetCommandWriter extends OffsetCommandWriter implements CommandWriter {

    private volatile GtidSet gtidSetContain;

    private RedisClientProtocol<Object[]> protocolParser;

    private RedisOpParser opParser;

    private CommandFileSegment lastSegment;

    private ControllableFile indexControllableFile;

    private int bytesBetweenIndex;

    private AtomicBoolean initialized = new AtomicBoolean(false);

    private static final Logger logger = LoggerFactory.getLogger(GtidSetCommandWriter.class);

    private static final int DEFAULT_BYTES_BETWEEN_INDEX = 50 * 1024 * 1024; // 50MB

    public GtidSetCommandWriter(RedisClientProtocol<Object[]> protocolParser, RedisOpParser opParser,
                                CommandStore cmdStore, int maxFileSize, Logger delayTraceLogger) {
        this(protocolParser, opParser, cmdStore, DEFAULT_BYTES_BETWEEN_INDEX, maxFileSize, delayTraceLogger);
    }

    public GtidSetCommandWriter(RedisClientProtocol<Object[]> protocolParser, RedisOpParser opParser,
                                CommandStore cmdStore, int bytesBetweenIndex, int maxFileSize, Logger delayTraceLogger) {
        super(cmdStore, maxFileSize, delayTraceLogger);
        this.protocolParser = protocolParser;
        this.opParser = opParser;
        this.bytesBetweenIndex = bytesBetweenIndex;
    }

    @Override
    public void initialize() throws IOException {
        if (initialized.compareAndSet(false, true)) {
            super.initialize();
            this.rotateIndexFile();

            this.lastSegment = getCommandStore().findLastFileSegment();
            this.gtidSetContain = lastSegment.getStartIdx().getExcludedGtidSet().clone();
            GtidCmdOneSegmentReader oneSegmentReader =
                    new GtidCmdOneSegmentReader(getCommandStore(), lastSegment, protocolParser, opParser);
            try {
                logger.info("[initialize][beg] recover gtid set from segment {}", lastSegment);
                while (!oneSegmentReader.isFinish()) {
                    RedisOp redisOp = oneSegmentReader.read();
                    if (null == redisOp || StringUtil.isEmpty(redisOp.getOpGtid())) continue;
                    this.gtidSetContain.add(redisOp.getOpGtid());
                }
            } finally {
                logger.info("[initialize][end] current gtid set {}", gtidSetContain);
                oneSegmentReader.close();
            }
        }
    }

    @Override
    public int write(ByteBuf byteBuf) throws IOException {
        if (!initialized.get()) throw new IllegalStateException("should be initialized before write");

        final int bufStartIndex = byteBuf.readerIndex();
        GtidSet toAddGtidSet = new GtidSet("");
        int byteBeforeInsert = -1;
        GtidSet toInsertIndexGtidSet = null;

        while (byteBuf.readableBytes() > 0) {
            RedisClientProtocol<Object[]> protocol = protocolParser.read(byteBuf);
            if (null == protocol) continue;

            Object[] payload = protocol.getPayload();
            RedisOp redisOp = opParser.parse(payload);
            protocolParser.reset();

            if (!StringUtil.isEmpty(redisOp.getOpGtid())) {
                toAddGtidSet.add(redisOp.getOpGtid());

                if (needCreateIndex(byteBuf.readerIndex() - bufStartIndex)) { // only insert index after a gtid cmd
                    byteBeforeInsert = byteBuf.readerIndex() - bufStartIndex;
                    toInsertIndexGtidSet = gtidSetContain.clone().union(toAddGtidSet);
                }
            }
        }

        byteBuf.readerIndex(bufStartIndex);
        int wrote = super.write(byteBuf);
        gtidSetContain = gtidSetContain.union(toAddGtidSet);

        if (null != toInsertIndexGtidSet && byteBeforeInsert > 0) {
            if (byteBeforeInsert == byteBuf.readableBytes()) {
                // rotate cmd file avoid index beyond cmd file
                // then index will point to the start of new cmd file
                rotateFileIfNecessary();
            }

            Pair<CommandFile, Long> writePosition = getWritePosition();
            CommandFileOffsetGtidIndex index = new CommandFileOffsetGtidIndex(toInsertIndexGtidSet, writePosition.getKey(),
                    writePosition.getValue() - wrote + byteBeforeInsert);
            tryInsertIndex(index);
        }

        return wrote;
    }

    @Override
    public synchronized boolean rotateFileIfNecessary() throws IOException {
        if (!initialized.get()) throw new IllegalStateException("should be initialized before rotate");

        if (super.rotateFileIfNecessary()) {
            rotateIndexFile();
            return true;
        }
        return false;
    }

    public GtidSet getGtidSetContain() {
        return gtidSetContain.clone();
    }

    private synchronized void rotateIndexFile() throws IOException {
        CommandFile commandFile = getWritePosition().getKey();
        File indexFile = getCommandStore().findIndexFile(commandFile);
        tryCloseFile(indexControllableFile);
        this.indexControllableFile = new DefaultControllableFile(indexFile);
    }


    private boolean needCreateIndex(int byteBeforeIndex) throws IOException {
        CommandFileOffsetGtidIndex lastIndex = lastSegment.getStartIdx();
        Pair<CommandFile, Long> writePosition = getWritePosition();
        CommandFile lastFile = lastIndex.getCommandFile();
        long lastPosition = lastIndex.getFileOffset();
        CommandFile curFile = writePosition.getKey();
        long curPosition = writePosition.getValue();

        return !lastFile.getFile().equals(curFile.getFile()) // insert index immediately if rotated to another cmd file
                || curPosition + byteBeforeIndex >= lastPosition + bytesBetweenIndex;
    }

    private synchronized void tryInsertIndex(CommandFileOffsetGtidIndex index) throws IOException {
        try {
            this.indexControllableFile.getFileChannel().write(ByteBuffer.wrap((index.buildIdxStr() + LINE_SEPARATOR).getBytes()));
        } catch (IOException ioException) {
            logger.info("[tryInsertIndex][fail] {}", index, ioException);
        }
        getCommandStore().addIndex(index);
        this.lastSegment = getCommandStore().findLastFileSegment();
    }

    private void tryCloseFile(ControllableFile file) {
        if (null == file) return;

        try {
            logger.debug("[tryCloseFile]{}", file);
            file.close();
        } catch (IOException e) {
            logger.error("[tryCloseFile]" + file, e);
        }
    }

    @VisibleForTesting
    public void setBytesBetweenIndex(int bytesBetweenIndex) {
        this.bytesBetweenIndex = bytesBetweenIndex;
    }

}
