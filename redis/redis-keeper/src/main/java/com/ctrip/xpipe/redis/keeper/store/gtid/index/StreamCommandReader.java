package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

public class StreamCommandReader {

    long currentOffset;

    private RedisClientProtocol<Object[]> protocolParser;
    private RedisOpParser opParser;
    private ByteBuf remainingBuf;

    private List<StreamCommandListener> listeners;

    public StreamCommandReader(long offset, RedisOpParser opParser) {
        this.currentOffset = offset;
        this.protocolParser = new ArrayParser();
        this.opParser = opParser;
        this.remainingBuf = null;
        this.listeners = new ArrayList<>();
    }

    public void addListener(StreamCommandListener listener) {
        this.listeners.add(listener);
    }

    public void doRead(ByteBuf byteBuf) {

        if (opParser == null) {
            throw new XpipeRuntimeException("unlikely: opParser is null");
        }

        CompositeByteBuf mergeBuf = Unpooled.compositeBuffer();
        if(remainingBuf != null && remainingBuf.readableBytes() > 0) {
            mergeBuf.addComponent(true, remainingBuf);
            remainingBuf = null;
        }
        mergeBuf.addComponent(true, byteBuf);
        int length = mergeBuf.readableBytes();
        while (mergeBuf.readableBytes() > 0) {
            int pre = mergeBuf.readerIndex();
            RedisClientProtocol<Object[]> protocol = protocolParser.read(mergeBuf);
            if (protocol == null) {
                this.protocolParser.reset();
                remainingBuf = mergeBuf.copy(pre, length - pre);
                break;
            }

            Object[] payload = protocol.getPayload();
            String gtid = readGtid(payload);
            if (!StringUtil.isEmpty(gtid)) {
                for(StreamCommandListener listener : listeners) {
                    listener.onCommand(gtid, this.currentOffset);
                }
            }
            // todo记录结束的 offset 吧
            this.currentOffset += mergeBuf.readerIndex() - pre;
            this.protocolParser.reset();
        }
    }

    public void resetOffset() {
        this.currentOffset = 0;
        if(this.remainingBuf != null && this.remainingBuf.readableBytes() > 0) {
            this.currentOffset -= remainingBuf.readableBytes();
        }
    }

    private String readGtid(Object[] payload) {
        if(payload == null || payload.length <= 2) {
            return null;
        }
        if(StringUtil.trimEquals("GTID", payload[0].toString())) {
            return payload[1].toString();
        } else {
            return null;
        }

    }



}
