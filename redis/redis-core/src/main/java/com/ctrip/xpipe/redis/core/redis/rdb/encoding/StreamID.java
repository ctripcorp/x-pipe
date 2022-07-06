package com.ctrip.xpipe.redis.core.redis.rdb.encoding;

import com.ctrip.xpipe.redis.core.redis.exception.RdbStreamParseFailException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2022/6/22
 */
public class StreamID {

    public static final int SIZE_STREAM_ID = 16; // 8 bytes ms + 8 bytes seq

    private long ms;

    private long seq;

    public StreamID(byte[] idBytes) {
        this(Unpooled.wrappedBuffer(idBytes));
    }

    public StreamID(ByteBuf idByteBuf) {
        if (idByteBuf.readableBytes() != SIZE_STREAM_ID) {
            throw new RdbStreamParseFailException("unexpected stream ID size " + idByteBuf.readableBytes());
        }

        ms = idByteBuf.readLong();
        seq = idByteBuf.readLong();
    }

    public StreamID(long ms, long seq) {
        this.ms = ms;
        this.seq = seq;
    }

    public long getMs() {
        return ms;
    }

    public long getSeq() {
        return seq;
    }

    @Override
    public String toString() {
        return String.format("%d-%d", ms, seq);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamID streamID = (StreamID) o;
        return ms == streamID.ms &&
                seq == streamID.seq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ms, seq);
    }
}
