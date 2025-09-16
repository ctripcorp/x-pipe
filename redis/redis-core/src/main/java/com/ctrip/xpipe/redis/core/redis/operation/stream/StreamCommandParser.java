package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

import static com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol.ASTERISK_BYTE;

public class StreamCommandParser {

    private static final Logger log = LoggerFactory.getLogger(StreamCommandParser.class);

    private RedisClientProtocol<Object[]> protocolParser;

    private RedisOpParser opParser;

    private ByteBuf remainingBuf;


    private StreamCommandLister commandLister;

    private ByteBufAllocator allocator;

    public StreamCommandParser(RedisOpParser opParser, StreamCommandLister commandLister) {
        this.protocolParser = new ArrayParser();
        this.commandLister = commandLister;
        allocator = ByteBufAllocator.DEFAULT;
        this.opParser = opParser;
    }


    public void doRead(ByteBuf byteBuf) throws IOException {

        try {
            while (byteBuf.readableBytes() > 0) {
                int pre = byteBuf.readerIndex();
                RedisClientProtocol<Object[]> protocol = null;
                try {
                    protocol = protocolParser.read(byteBuf);
                } catch (RedisRuntimeException | NumberFormatException exception) {
                    // 错误处理，碰到脏数据需要跳过，预期不会用脏数据
                    log.error("[doRead] {}", exception.getMessage());
                    int starIndex = findFirstStar(byteBuf, pre + 1);
                    if(starIndex != -1) {
                        log.info("[skip some data] from {}", byteBuf.slice(pre, starIndex - pre).toString(Charset.defaultCharset()));
                        byteBuf.readerIndex(starIndex);
                        this.reset();
                        continue;
                    } else {
                        throw exception;
                    }
                }
                if (protocol == null) {
                    ByteBuf  newRemainingBuf = allocator.buffer(getRemainLength() + byteBuf.writerIndex() - pre);
                    if(remainingBuf != null && remainingBuf.readableBytes() > 0) {
                        newRemainingBuf.writeBytes(remainingBuf);
                    }
                    newRemainingBuf.writeBytes(byteBuf.slice(pre,  byteBuf.writerIndex() - pre));
                    this.relaseReaminBuf();
                    this.remainingBuf = newRemainingBuf;
                    break;
                }

                CompositeByteBuf mergeBuf = allocator.compositeBuffer();
                try {
                    if(remainingBuf != null && remainingBuf.readableBytes() > 0) {
                        mergeBuf.addComponent(true, remainingBuf);
                        remainingBuf = null; // 旧的引用置空，其生命周期由mergeBuf管理
                    }
                    ByteBuf finishBuf = byteBuf.slice(pre, byteBuf.readerIndex() - pre);
                    finishBuf.retain();
                    mergeBuf.addComponent(true, finishBuf);
                    Object[] payload = protocol.getPayload();
                    commandLister.onCommand(payload, mergeBuf);
                } finally {
                    mergeBuf.release();
                }
                this.protocolParser.reset();
            }
        } catch (Exception e) {
            this.reset();
            this.protocolParser.reset();
        }
        finally {
            byteBuf.skipBytes(byteBuf.readableBytes());
        }
    }

    private int findFirstStar(ByteBuf byteBuf, int beginOffset) {
        for(int i = beginOffset; i < byteBuf.writerIndex(); i++) {
            byte b = byteBuf.getByte(i);
            if(b == ASTERISK_BYTE) {
                return i;
            }
        }
        return -1;
    }

    private void relaseReaminBuf() {
        if(remainingBuf != null) {
            remainingBuf.release();
            remainingBuf = null;
        }
    }

    public void reset() {
        if(remainingBuf != null) {
            this.remainingBuf.release();
            this.remainingBuf = null;
        }
        this.protocolParser.reset();
    }

    public int getRemainLength() {
        if(remainingBuf == null) {
            return 0;
        }
        return this.remainingBuf.readableBytes();
    }
}