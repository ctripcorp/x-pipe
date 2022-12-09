package com.ctrip.xpipe.redis.core.redis.rdb;

import io.netty.buffer.ByteBuf;

/**
 * @author lishanglin
 * date 2022/5/28
 */
public interface RdbParser<T> {

    void skip(boolean skip);

    void needFinishNotify(boolean need);

    T read(ByteBuf byteBuf);

    boolean isFinish();

    void reset();

    void registerListener(RdbParseListener listener);

    void unregisterListener(RdbParseListener listener);

}
