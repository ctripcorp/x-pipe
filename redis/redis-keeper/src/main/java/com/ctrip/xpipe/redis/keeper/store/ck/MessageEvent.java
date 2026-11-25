package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.redis.core.redis.operation.IRedisOpItem;

// 1. 定义事件类
public class MessageEvent {

    private IRedisOpItem redisOpItem;

    private long timestamp;

    public IRedisOpItem getRedisOpItem() {
        return redisOpItem;
    }

    public void setRedisOpItem(IRedisOpItem redisOpItem) {
        this.redisOpItem = redisOpItem;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
