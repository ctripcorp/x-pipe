package com.ctrip.xpipe.redis.keeper.store.ck;

import java.util.List;

public class RedisOpMultiItem implements IRedisOpItem<List<RedisOpItem>>{
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
        for(RedisOpItem redisOpItem:redisOpItems){
            redisOpItem.clear();
        }
        redisOpItems.clear();
    }
}
