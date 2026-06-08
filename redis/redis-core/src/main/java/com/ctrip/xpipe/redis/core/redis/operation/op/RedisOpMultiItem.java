package com.ctrip.xpipe.redis.core.redis.operation.op;

import com.ctrip.xpipe.redis.core.redis.operation.IRedisOpItem;

import java.util.List;

public class RedisOpMultiItem implements IRedisOpItem<List<RedisOpItem>> {
    private List<RedisOpItem> redisOpItems;

    public RedisOpMultiItem(List<RedisOpItem> redisOpItems){
        this.redisOpItems = redisOpItems;
    }

    @Override
    public List<RedisOpItem> getRedisOpItem() {
        return redisOpItems;
    }

    @Override
    public void clear() {
        if (redisOpItems == null) {
            return;
        }
        for (RedisOpItem redisOpItem : redisOpItems) {
            redisOpItem.clear();
        }
        redisOpItems = null;
    }
}
