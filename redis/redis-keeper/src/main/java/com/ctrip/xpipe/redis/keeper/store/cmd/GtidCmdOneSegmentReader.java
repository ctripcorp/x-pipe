package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lishanglin
 * date 2022/5/21
 * read cmd in one segment
 * if segment is right-bound-open read tilt the end of cmd files
 */
public class GtidCmdOneSegmentReader implements CommandReader<RedisOp> {

    private CommandFile curCmdFile;

    ByteBuf curBuf = null;

    private ControllableFile controllableFile;

    private CommandStore<?,?> commandStore;

    private CommandFileSegment segment;

    private RedisClientProtocol<Object[]> protocolParser;

    private RedisOpParser opParser;

    private AtomicBoolean finished = new AtomicBoolean(false);

    private static final Logger logger = LoggerFactory.getLogger(GtidCmdOneSegmentReader.class);

    private static final int FILE_BUFFER_SIZE = 4096;

    public GtidCmdOneSegmentReader(CommandStore<?,?> commandStore, CommandFileSegment segment,
                                   RedisClientProtocol<Object[]> protocolParser, RedisOpParser opParser) throws IOException {
        this.commandStore = commandStore;
        this.segment = segment;
        this.protocolParser = protocolParser;
        this.opParser = opParser;

        CommandFileOffsetGtidIndex startIndex = segment.getStartIdx();
        this.setCmdFile(startIndex.getCommandFile(), startIndex.getFileOffset());
    }

    @Override
    public RedisOp read() throws IOException {
        if (finished.get()) return null;

        readNextFileIfNecessary();
        refillBufIfNecessary();

        if (finished.get()) return null;

        RedisClientProtocol<Object[]> protocol = protocolParser.read(curBuf);
        if (null == protocol) return null;

        Object[] payload = protocol.getPayload();
        RedisOp redisOp = opParser.parse(Stream.of(payload).map(Object::toString).collect(Collectors.toList()));
        this.protocolParser.reset();
        return redisOp;
    }

    private void readNextFileIfNecessary() throws IOException {
        commandStore.makeSureOpen();

        if (controllableFile.getFileChannel().position() >= controllableFile.size() && curBuf.readableBytes() <= 0) {
            CommandFile nextCommandFile = commandStore.findNextFile(curCmdFile.getFile());
            if (nextCommandFile != null) {
                setCmdFile(nextCommandFile, 0);
            } else {
                readEnd(); // no more data to read
            }
        }
    }

    private void refillBufIfNecessary() throws IOException {
        if (finished.get()) return;
        if (null != curBuf && curBuf.readableBytes() > 0) return;

        ByteBuffer cmdBuffer;
        long curFilePosition = filePosition();
        if (!segment.rightBoundOpen() && segment.getEndIdx().getCommandFile().equals(curCmdFile)) {
            long endOffset = segment.getEndIdx().getFileOffset();
            if (endOffset == curFilePosition) {
                readEnd();
                return;
            } else if(endOffset < curFilePosition) {
                throw new XpipeRuntimeException("read beyond segment " + segment + ", current position " + curFilePosition);
            }

            cmdBuffer = ByteBuffer.allocateDirect((int)Math.max(Math.min(FILE_BUFFER_SIZE, endOffset - curFilePosition), 0));
        } else {
            cmdBuffer = ByteBuffer.allocateDirect(FILE_BUFFER_SIZE);
        }

        curBuf = Unpooled.wrappedBuffer(cmdBuffer);
        controllableFile.getFileChannel().read(cmdBuffer);
        if (cmdBuffer.position() < cmdBuffer.capacity()) curBuf.capacity(cmdBuffer.position());
    }

    private long filePosition() throws IOException {
        return controllableFile.getFileChannel().position();
    }

    private synchronized void setCmdFile(CommandFile cmdFile, long filePosition) throws IOException {
        tryCloseFile(controllableFile);
        this.curCmdFile = cmdFile;
        this.controllableFile = new DefaultControllableFile(cmdFile.getFile());

        try {
            this.controllableFile.getFileChannel().position(filePosition);
        } catch (IOException ioException) {
            tryCloseFile(this.controllableFile);
            throw ioException;
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
    public void close() {
        readEnd();
    }

    private void readEnd() {
        if (finished.compareAndSet(false, true)) {
            tryCloseFile(controllableFile);
        }
    }

    public boolean isFinish() {
        return finished.get();
    }

    @Override
    public void flushed(RedisOp cmdContent) {
        // do nothing
    }

    @Override
    public File getCurFile() {
        return curCmdFile.getFile();
    }
}
