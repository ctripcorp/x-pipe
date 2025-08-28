package com.ctrip.xpipe.redis.core.redis.operation.stream;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
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

    public StreamCommandParser(RedisOpParser opParser, StreamCommandLister commandLister) {
        this.protocolParser = new ArrayParser();
        this.opParser = opParser;
        this.commandLister = commandLister;
    }


    public void doRead(ByteBuf byteBuf) throws IOException {

        if (opParser == null) {
            throw new XpipeRuntimeException("unlikely: opParser is null");
        }
        CompositeByteBuf mergeBuf = Unpooled.compositeBuffer();
        if(remainingBuf != null && remainingBuf.readableBytes() > 0) {
            mergeBuf.addComponent(true, remainingBuf);
            remainingBuf = null;
        }
        mergeBuf.addComponent(true, byteBuf);
        while (mergeBuf.readableBytes() > 0) {
            int pre = mergeBuf.readerIndex();
            RedisClientProtocol<Object[]> protocol = null;
            try {
                protocol = protocolParser.read(mergeBuf);
            } catch (RedisRuntimeException | NumberFormatException exception) {
                log.error("[doRead] {}", exception.getMessage());
                int starIndex = findFirstStar(mergeBuf, pre + 1);
                if(starIndex != -1) {
                    log.info("[skip some data] from {}", mergeBuf.slice(pre, starIndex - pre).toString(Charset.defaultCharset()));
                    mergeBuf.readerIndex(starIndex);
                    this.protocolParser.reset();
                    continue;
                }
            }
            if (protocol == null) {
                this.protocolParser.reset();
                this.relaseRemainBuf();
                remainingBuf = Unpooled.copiedBuffer(mergeBuf.slice(pre,  mergeBuf.writerIndex() - pre));
                break;
            }

            ByteBuf finishBuf = mergeBuf.slice(pre, mergeBuf.readerIndex() - pre);

            Object[] payload = protocol.getPayload();
            commandLister.onCommand(payload, finishBuf);
            this.protocolParser.reset();
        }
        byteBuf.skipBytes(byteBuf.readableBytes());
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

    public void relaseRemainBuf() {
        this.remainingBuf = null;
    }

    public long getRemainLength() {
        if(remainingBuf == null) {
            return 0;
        }
        return this.remainingBuf.readableBytes();
    }
}
