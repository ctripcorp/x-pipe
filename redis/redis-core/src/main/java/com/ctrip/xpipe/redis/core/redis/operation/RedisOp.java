package com.ctrip.xpipe.redis.core.redis.operation;

import io.netty.buffer.ByteBuf;

/**
 * @author lishanglin
 * date 2022/2/17
 */
public interface RedisOp {

    RedisOpType getOpType();

    String getDbId();

    String getOpGtid();

    Long getTimestamp();

    String getGid();

    byte[][] buildRawOpArgs();

    ByteBuf buildRESP();

    //overhead apart
    long estimatedSize();

    void clearGtid();
}
