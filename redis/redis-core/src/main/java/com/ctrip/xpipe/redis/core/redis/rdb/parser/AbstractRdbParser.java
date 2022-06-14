package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLenType;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbLength;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.google.common.primitives.UnsignedLong;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/5/28
 */
public abstract class AbstractRdbParser<T> implements RdbParser<T> {

    private Set<RdbParseListener> listeners = new HashSet<>();

    private LEN_READ_STATE lenReadState = LEN_READ_STATE.READ_INIT;

    private RdbLenType lenType;

    private ByteBuf lenTemp;

    private int lenNeedBytes = -1;

    private boolean skipParse = false;

    private boolean needFinishNotify = false;

    protected abstract Logger getLogger();

    enum LEN_READ_STATE {
        READ_INIT,
        READ_TYPE,
        READ_VALUE
    }

    protected int skip(ByteBuf byteBuf, int skip) {
        int toSkip = Math.max(0, Math.min(byteBuf.readableBytes(), skip));
        byteBuf.readerIndex(byteBuf.readerIndex() + toSkip);
        return toSkip;
    }

    protected RdbLength parseRdbLength(ByteBuf byteBuf) {

        while(byteBuf.readableBytes() > 0) {

            switch (lenReadState) {
                case READ_INIT:
                    lenType = null;
                    lenTemp = null;
                    lenNeedBytes = -1;
                    lenReadState = LEN_READ_STATE.READ_TYPE;
                    break;

                case READ_TYPE:
                    short lenTypeRaw = byteBuf.getUnsignedByte(byteBuf.readerIndex());
                    lenType = RdbLenType.parse(lenTypeRaw);
                    if (null == lenType) throw new XpipeRuntimeException("unknown len type " + lenTypeRaw);

                    if (lenType.needSkipLenTypeByte()) byteBuf.readerIndex(byteBuf.readerIndex() + 1);
                    lenReadState = LEN_READ_STATE.READ_VALUE;
                    break;

                case READ_VALUE:
                    if (lenNeedBytes < 0) {
                        lenNeedBytes = ((lenType.needSkipLenTypeByte() ? 0 : 2) + lenType.getBitsAfterTypeBits()) / 8;
                    }
                    lenTemp = readUntilBytesEnough(byteBuf, lenTemp, lenNeedBytes);
                    if (lenTemp.readableBytes() == lenNeedBytes) {
                        int lenValue = parseUnsignedLong(lenTemp, lenType);
                        RdbLength rdbLength = new RdbLength(lenType, lenValue);
                        lenReadState = LEN_READ_STATE.READ_INIT;
                        lenTemp.release();
                        lenTemp = null;
                        return rdbLength;
                    }
            }
        }

        return null;
    }

    protected ByteBuf readUntilBytesEnough(ByteBuf src, ByteBuf dst, int needReadBytes) {
        if (null == dst && src.readableBytes() >= needReadBytes) {
            return src.readBytes(needReadBytes);
        } else if (null == dst) {
            CompositeByteBuf newByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(needReadBytes);
            newByteBuf.addComponent(true, src.readBytes(src.readableBytes()));
            return newByteBuf;
        } else {
            int readCnt = Math.min(src.readableBytes(), needReadBytes - dst.readableBytes());
            if (dst instanceof CompositeByteBuf) {
                ((CompositeByteBuf)dst).addComponent(true, src.readBytes(readCnt));
                return dst;
            } else {
                CompositeByteBuf newByteBuf = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(needReadBytes);
                newByteBuf.addComponents(true, dst, src.readBytes(readCnt));
                return newByteBuf;
            }
        }
    }

    private int parseUnsignedLong(ByteBuf byteBuf, RdbLenType lenType) {
        boolean hasSkipTypeBits = lenType.needSkipLenTypeByte();
        long rawLong = 0;
        while (byteBuf.readableBytes() > 0) {
            int unsignedByte = byteBuf.readUnsignedByte();
            if (!hasSkipTypeBits) {
                unsignedByte = 0x3f & unsignedByte;
                hasSkipTypeBits = true;
            }
            rawLong = (rawLong << 8) | unsignedByte;
        }

        if (rawLong < 0 || rawLong > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException("unsupport huge len " + UnsignedLong.fromLongBits(rawLong));
        }
        return (int) rawLong;
    }

    protected void notifyRedisOp(RedisOp redisOp) {
        getLogger().debug("[notifyRedisOp] {}", redisOp);
        for (RdbParseListener listener: listeners) {
            try {
                listener.onRedisOp(redisOp);
            } catch (Throwable th) {
                getLogger().info("[notifyRedisOp][fail][{}] {}", listener, redisOp, th);
            }
        }
    }

    protected void notifyAux(String key, String value) {
        getLogger().debug("[notifyAux] {} {}", key, value);
        for (RdbParseListener listener: listeners) {
            try {
                listener.onAux(key, value);
            } catch (Throwable th) {
                getLogger().info("[notifyAux][fail][{}] {} {}", listener, key, value, th);
            }
        }
    }

    protected void notifyFinish() {
        if (!needFinishNotify) return;

        getLogger().debug("[notifyFinish]");
        for (RdbParseListener listener: listeners) {
            try {
                listener.onFinish(this);
            } catch (Throwable th) {
                getLogger().info("[notifyFinish][fail] {}", listener, th);
            }
        }
    }

    protected boolean isSkipParse() {
        return skipParse;
    }

    @Override
    public void skip(boolean skip) {
        this.skipParse = skip;
    }

    @Override
    public void needFinishNotify(boolean need) {
        this.needFinishNotify = need;
    }

    @Override
    public void registerListener(RdbParseListener listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(RdbParseListener listener) {
        listeners.remove(listener);
    }
}
