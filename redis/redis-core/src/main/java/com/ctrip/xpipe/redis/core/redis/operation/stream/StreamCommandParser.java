package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
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

    private CompositeByteBuf remainingBuf;

    private StreamCommandLister commandLister;

    private ByteBufAllocator allocator;

    public StreamCommandParser(StreamCommandLister commandLister) {
        this.protocolParser = new ArrayParser();
        allocator = ByteBufAllocator.DEFAULT;
        this.commandLister = commandLister;
    }


    public void doRead(ByteBuf byteBuf) throws IOException {

        try {
            while (byteBuf.readableBytes() > 0) {
                int pre = byteBuf.readerIndex();
                RedisClientProtocol<Object[]> protocol = null;
                try {
                    protocol = protocolParser.read(byteBuf);
                } catch (RedisRuntimeException | NumberFormatException exception) {
                    // 错误处理，碰到脏数据需要跳过，预期不会有脏数据
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

                // copy current buf as we don't know whether the bytebuf will be reused or not in next code
                copyAndComposite(byteBuf, pre);

                if (protocol == null) {
                    break;
                }

                try {
                    Object[] payload = protocol.getPayload();
                    commandLister.onCommand(payload, remainingBuf);
                } finally {
                    relaseReaminBuf();
                }
                this.protocolParser.reset();
            }
        } catch (Exception e) {
            log.error("[doRead] error", e);
            this.reset();
        } finally {
            byteBuf.skipBytes(byteBuf.readableBytes());
        }
    }

    private void copyAndComposite(ByteBuf byteBuf, int pre) {
        if(remainingBuf == null) {
            remainingBuf = allocator.compositeBuffer();
        }
        ByteBuf copy = allocator.buffer(byteBuf.writerIndex() - pre);
        copy.writeBytes(byteBuf.slice(pre, byteBuf.writerIndex() - pre));
        remainingBuf.addComponent(true, copy);
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
        relaseReaminBuf();
        this.protocolParser.reset();
    }

    public int getRemainLength() {
        if(remainingBuf == null) {
            return 0;
        }
        return this.remainingBuf.readableBytes();
    }
}