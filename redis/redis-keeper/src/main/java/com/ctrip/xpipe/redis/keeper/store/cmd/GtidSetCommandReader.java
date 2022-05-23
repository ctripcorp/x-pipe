package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.OffsetNotifier;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lishanglin
 * date 2022/5/5
 */
public class GtidSetCommandReader extends AbstractFlyingThresholdCommandReader<RedisOp> implements CommandReader<RedisOp> {

    private CommandStore<?, RedisOp> commandStore;

    private CommandFile curCmdFile;

    ByteBuf curBuf = null;

    private ControllableFile controllableFile;

    private RedisClientProtocol<Object[]> protocolParser;

    private RedisOpParser opParser;

    private OffsetNotifier offsetNotifier;

    private CommandFileSegment currentFileSegment;

    private GtidSet excludedGtidSet;

    private Set<String> interestedSrcIds;

    private Logger logger = LoggerFactory.getLogger(GtidSetCommandReader.class);

    private static final int FILE_BUFFER_SIZE = Integer.parseInt(System.getProperty("CMD_FILE_BUFFER_SIZE", "4096"));

    public GtidSetCommandReader(CommandStore<?, RedisOp> commandStore, GtidSet excludedGtidSet,
                                RedisClientProtocol<Object[]> protocolParser, RedisOpParser opParser,
                                OffsetNotifier offsetNotifier, long flyingThreshold) throws IOException {
        super(commandStore, flyingThreshold);
        this.commandStore = commandStore;
        this.offsetNotifier = offsetNotifier;
        this.excludedGtidSet = excludedGtidSet.clone();
        this.protocolParser = protocolParser;
        this.opParser = opParser;
        this.interestedSrcIds = excludedGtidSet.getUUIDs();
        this.currentFileSegment = this.commandStore.findFirstFileSegment(excludedGtidSet);

        CommandFileOffsetGtidIndex startIndex = currentFileSegment.getStartIdx();
        this.setCmdFile(startIndex.getCommandFile(), startIndex.getFileOffset(), false);
    }

    @Override
    public RedisOp doRead() throws IOException {
        rollToNextSegmentIfNecessary();
        readNextFileIfNecessary();
        refillBufIfNecessary();

        RedisClientProtocol<Object[]> protocol = protocolParser.read(curBuf);
        if (null == protocol) return null;

        Object[] payload = protocol.getPayload();
        RedisOp redisOp = opParser.parse(Stream.of(payload).map(Object::toString).collect(Collectors.toList()));
        if (!StringUtil.isEmpty(redisOp.getOpGtid())) excludedGtidSet.add(redisOp.getOpGtid());
        this.protocolParser.reset();
        return redisOp;
    }

    private void refillBufIfNecessary() throws IOException {
        if (null != curBuf && curBuf.readableBytes() > 0) return;

        ByteBuffer cmdBuffer;
        if (!currentFileSegment.rightBoundOpen() && currentFileSegment.getEndIdx().getCommandFile().equals(curCmdFile)) {
            long endOffset = currentFileSegment.getEndIdx().getFileOffset();
            cmdBuffer = ByteBuffer.allocateDirect((int)Math.max(Math.min(FILE_BUFFER_SIZE, endOffset - filePosition()), 0));
        } else {
            tryWaitOffset();
            cmdBuffer = ByteBuffer.allocateDirect(FILE_BUFFER_SIZE);
        }

        curBuf = Unpooled.wrappedBuffer(cmdBuffer);
        controllableFile.getFileChannel().read(cmdBuffer);
        if (cmdBuffer.position() < cmdBuffer.capacity()) curBuf.capacity(cmdBuffer.position());
    }

    private void tryWaitOffset() throws IOException {
        try {
            long globalOffset = curCmdFile.getStartOffset() + filePosition();
            offsetNotifier.await(globalOffset);
        } catch (InterruptedException e) {
            logger.info("[doRead]", e);
            Thread.currentThread().interrupt();
        }
    }

    private void rollToNextSegmentIfNecessary() throws IOException {
        if (currentFileSegment.rightBoundOpen()) {
            return;
        }

        // data in segment has been read completely
        boolean needSkipCurrentSegment = curCmdFile.equals(currentFileSegment.getEndIdx().getCommandFile())
                && filePosition() >= currentFileSegment.getEndIdx().getFileOffset() && curBuf.readableBytes() <= 0;

        if (!needSkipCurrentSegment) {
            // needed gtid cmd in segment has been read completely
            GtidSet leftBoundExcludedGtidSet = currentFileSegment.getStartIdx().getExcludedGtidSet().filterGtid(interestedSrcIds);
            GtidSet rightBoundExcludedGtidSet = currentFileSegment.getEndIdx().getExcludedGtidSet().filterGtid(interestedSrcIds);
            GtidSet includedGtidSet = rightBoundExcludedGtidSet.subtract(leftBoundExcludedGtidSet);
            needSkipCurrentSegment = includedGtidSet.isContainedWithin(excludedGtidSet);
        }

        if (needSkipCurrentSegment) {
            CommandFileSegment nextFileSegment = this.commandStore.findFirstFileSegment(excludedGtidSet);
            logger.info("[findNextSegmentIfNeeded][{}] next segment {}", excludedGtidSet, nextFileSegment);

            CommandFileOffsetGtidIndex startIndex = nextFileSegment.getStartIdx();
            setCmdFile(startIndex.getCommandFile(), startIndex.getFileOffset(), true);
            this.currentFileSegment = nextFileSegment;
        }
    }

    private void readNextFileIfNecessary() throws IOException {
        commandStore.makeSureOpen();

        if (filePosition() >= controllableFile.size() && curBuf.readableBytes() <= 0) {
            CommandFile nextCommandFile = commandStore.findNextFile(curCmdFile.getFile());
            if (nextCommandFile != null) {
                setCmdFile(nextCommandFile, 0, false);
            }
        }
    }

    private long filePosition() throws IOException {
        return controllableFile.getFileChannel().position();
    }

    private synchronized void setCmdFile(CommandFile cmdFile, long filePosition, boolean clearBuf) throws IOException {
        tryCloseFile(controllableFile);
        this.curCmdFile = cmdFile;
        this.controllableFile = new DefaultControllableFile(cmdFile.getFile());

        try {
            this.controllableFile.getFileChannel().position(filePosition);
        } catch (IOException ioException) {
            tryCloseFile(this.controllableFile);
            throw ioException;
        }

        if (clearBuf) {
            this.curBuf = null;
            this.protocolParser.reset();
        }
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

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public File getCurFile() {
        return curCmdFile.getFile();
    }


    @Override
    public void close() throws IOException {
        commandStore.removeReader(this);
        tryCloseFile(controllableFile);
    }

}
