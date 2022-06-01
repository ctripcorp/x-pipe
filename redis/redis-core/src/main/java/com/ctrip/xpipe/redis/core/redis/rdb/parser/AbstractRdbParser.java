package com.ctrip.xpipe.redis.core.redis.rdb.parser;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.google.common.primitives.UnsignedLong;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * @author lishanglin
 * date 2022/5/28
 */
public abstract class AbstractRdbParser implements RdbParser {

    private Set<RdbParseListener> listeners = new HashSet<>();

    private boolean skipParse = false;

    protected abstract Logger getLogger();

    enum STATE {
        READ_TYPE,
        READ_VALUE,
        READ_CLEN,
        READ_LEN,
        READ_LZF_VALUE,
        LZF_DECOMPRESS
    }

    protected int skip(ByteBuf byteBuf, int skip) {
        int toSkip = Math.max(0, Math.min(byteBuf.readableBytes(), skip));
        byteBuf.readerIndex(byteBuf.readerIndex() + toSkip);
        return toSkip;
    }

    protected UnsignedLong decodeLength(ByteBuf byteBuf) {
        return UnsignedLong.fromLongBits(byteBuf.readLong());
    }

    protected void notifyRedisOp(RedisOp redisOp) {

    }

    protected void notifyAux(String key, String value) {

    }

    protected void notifyFinish() {
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
    public void registerListener(RdbParseListener listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(RdbParseListener listener) {
        listeners.remove(listener);
    }
}
