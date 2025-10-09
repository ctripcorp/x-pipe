package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
                RedisClientProtocol<Object[]> protocol = protocolParser.read(byteBuf);
                compositeCurrentPayLoadBuffer(byteBuf, pre);
                if (protocol == null) {
                    break;
                } else {
                    try {
                        Object[] payload = protocol.getPayload();
                        commandLister.onCommand(payload, remainingBuf);
                    } finally {
                        relaseReaminBuf();
                    }
                    this.protocolParser.reset();
                }
            }
        } catch (Throwable e) {
            log.error("[doRead]", e);
            this.reset();
        } finally {
            byteBuf.skipBytes(byteBuf.readableBytes());
        }
    }

    private void compositeCurrentPayLoadBuffer(ByteBuf byteBuf, int pre) {
        if (remainingBuf == null) {
            remainingBuf = allocator.compositeBuffer();
        }
        ByteBuf slice = byteBuf.slice(pre, byteBuf.readerIndex() - pre);
        slice.retain();
        remainingBuf.addComponent(true, slice);
    }

    private void relaseReaminBuf() {
        if (remainingBuf != null) {
            remainingBuf.release();
            remainingBuf = null;
        }
    }

    public void reset() {
        relaseReaminBuf();
        this.protocolParser.reset();
    }

    public int getRemainLength() {
        if (remainingBuf == null) {
            return 0;
        }
        return this.remainingBuf.readableBytes();
    }
}