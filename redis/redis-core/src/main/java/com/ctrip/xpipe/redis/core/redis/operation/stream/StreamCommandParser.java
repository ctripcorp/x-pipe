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


public class StreamCommandParser {

    private static final Logger log = LoggerFactory.getLogger(StreamCommandParser.class);

    private RedisClientProtocol<Object[]> protocolParser;

    private CompositeByteBuf remainingBuf;

    private StreamCommandLister commandLister;

    private ByteBufAllocator allocator;

    private boolean shouldFindNextCommandStart;
    private int findNextCommandStartIndex;
    private final int MAX_LOG_LENGTH = 512;

    public StreamCommandParser(StreamCommandLister commandLister) {
        this.protocolParser = new ArrayParser();
        allocator = ByteBufAllocator.DEFAULT;
        this.commandLister = commandLister;
        this.shouldFindNextCommandStart = false;
        findNextCommandStartIndex = -1;
    }

    public void doRead(ByteBuf byteBuf) throws IOException {
        try {
            while (byteBuf.readableBytes() > 0) {
                // find some dirty data in last buf, try to find next command start in cur buf
                if(shouldFindNextCommandStart) {
                    int findStartIndex = findNextCommandStartIndex == -1 ? byteBuf.readerIndex() : findNextCommandStartIndex;
                    int startIndex = findNextCommandStart(byteBuf, findStartIndex);
                    if(startIndex != -1) {
                        if (log.isDebugEnabled()) {
                            log.info("[find next command start] at {}", startIndex);
                        }
                        byteBuf.readerIndex(startIndex);
                        this.reset();
                    } else {
                        if (log.isDebugEnabled()) {
                            // not found, skip all, max 512 chars
                            log.info("[skip all data] {}", byteBuf.slice(findStartIndex, Math.min(MAX_LOG_LENGTH, byteBuf.readableBytes())).toString(Charset.defaultCharset()));
                        }
                        findNextCommandStartIndex = -1;
                        return;
                    }
                }
                int pre = byteBuf.readerIndex();
                RedisClientProtocol<Object[]> protocol;
                try {
                    protocol = protocolParser.read(byteBuf);
                } catch (RedisRuntimeException | NumberFormatException exception) {
                    // dirty data find
                    log.error("[doRead] {}", exception.getMessage());
                    this.findNextCommandStartIndex = pre + 1;
                    setNeedFindNextCommandStart(true);
                    continue;
                }

                compositeCurrentPayLoadBuffer(byteBuf, pre);
                if (protocol == null) {
                    break;
                } else {
                    try {
                        Object[] payload = protocol.getPayload();
                        commandLister.onCommand(payload, remainingBuf);
                    } finally {
                        this.reset();
                    }
                }
            }
        } catch (Throwable e) {
            log.error("[doRead] error", e);
            this.reset();
            this.protocolParser.reset();
        } finally {
            byteBuf.skipBytes(byteBuf.readableBytes());
        }
    }

    private void setNeedFindNextCommandStart(boolean need) {
        this.shouldFindNextCommandStart = need;
    }

    private void compositeCurrentPayLoadBuffer(ByteBuf byteBuf, int pre) {
        if (remainingBuf == null) {
            remainingBuf = allocator.compositeBuffer();
        }
        ByteBuf slice = byteBuf.slice(pre, byteBuf.readerIndex() - pre);
        slice.retain();
        remainingBuf.addComponent(true, slice);
    }

    private int findNextCommandStart(ByteBuf byteBuf, int beginOffset) {
        for(int i = beginOffset; i < byteBuf.writerIndex(); i++) {
            byte b = byteBuf.getByte(i);
            if(b == RedisClientProtocol.ASTERISK_BYTE) {
                return i;
            }
        }
        return -1;
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
        setNeedFindNextCommandStart(false);
        this.findNextCommandStartIndex = -1;
    }

    public int getRemainLength() {
        if (remainingBuf == null) {
            return 0;
        }
        return this.remainingBuf.readableBytes();
    }
}