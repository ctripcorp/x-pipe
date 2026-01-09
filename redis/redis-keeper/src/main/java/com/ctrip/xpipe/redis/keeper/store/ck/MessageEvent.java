package com.ctrip.xpipe.redis.keeper.store.ck;

import java.util.List;

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
