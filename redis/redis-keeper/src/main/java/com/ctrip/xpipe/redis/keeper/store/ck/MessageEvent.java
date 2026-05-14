package com.ctrip.xpipe.redis.keeper.store.ck;

import com.ctrip.xpipe.redis.core.redis.operation.IRedisOpItem;

// 1. 定义事件类
public class MessageEvent {

    private IRedisOpItem redisOpItem;

    public IRedisOpItem getRedisOpItem() {
        return redisOpItem;
    }

    public void setRedisOpItem(IRedisOpItem redisOpItem) {
        this.redisOpItem = redisOpItem;
    }
}
