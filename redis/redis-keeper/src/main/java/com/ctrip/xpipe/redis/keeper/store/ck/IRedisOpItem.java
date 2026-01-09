package com.ctrip.xpipe.redis.keeper.store.ck;

public interface IRedisOpItem<T> {
    T getRedisOpItem();
    void clear();
}
