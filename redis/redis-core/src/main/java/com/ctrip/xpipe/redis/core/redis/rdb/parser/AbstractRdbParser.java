package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpType;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpSingleKey;
import com.ctrip.xpipe.redis.core.redis.rdb.*;
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

    private ByteBuf millSecondTemp;

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
                        long lenValue = parseUnsignedLong(lenTemp, lenType);
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

    protected Long readMillSecond(ByteBuf src, RdbParseContext context) {
        millSecondTemp = readUntilBytesEnough(src, millSecondTemp, 8);
        long millSecond;
        if (millSecondTemp.readableBytes() == 8) {
            if (context.getRdbVersion() >= 9) {
                millSecond = millSecondTemp.readLongLE();
            } else {
                millSecond = millSecondTemp.readLong();
            }
        } else return null;

        millSecondTemp.release();
        millSecondTemp = null;
        return millSecond;
    }

    protected void propagateExpireAtIfNeed(RedisKey redisKey, long expireTimeMilli) {
        if (null == redisKey || expireTimeMilli <= 0) return;

        byte[] expireAt = String.valueOf(expireTimeMilli).getBytes();
        notifyRedisOp(new RedisOpSingleKey(
                RedisOpType.PEXPIREAT, new byte[][] {
                RedisOpType.PEXPIREAT.name().getBytes(), redisKey.get(), expireAt},
                redisKey, expireAt));
    }

    private long parseUnsignedLong(ByteBuf byteBuf, RdbLenType lenType) {
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

        if (rawLong < 0) {
            throw new UnsupportedOperationException("unsupport unsigned long in rdb len");
        }
        return rawLong;
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

    protected void notifyAuxEnd() {
        getLogger().debug("[notifyAuxEnd]");
        for (RdbParseListener listener : listeners) {
            try {
                listener.onAuxFinish();
            } catch (Throwable t){
                getLogger().info("[notifyAuxEnd][fail][{}]", listener, t);
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

    @Override
    public void reset() {
        if (lenTemp != null) {
            lenTemp.release();
        }
        if (millSecondTemp != null){
            millSecondTemp.release();
        }
        this.lenReadState = LEN_READ_STATE.READ_INIT;
    }
}
