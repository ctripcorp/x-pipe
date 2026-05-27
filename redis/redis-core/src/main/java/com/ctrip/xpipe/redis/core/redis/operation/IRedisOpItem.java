package com.ctrip.xpipe.redis.core.redis.operation;

public interface IRedisOpItem<T> {
    T getRedisOpItem();
    void clear();
}
