package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

public class ShardMetaCrossRegionVisitor implements MetaVisitor<ShardMeta>{

    private RedisMetaVisitor redisMetaVisitor;

    public ShardMetaCrossRegionVisitor(RedisMetaVisitor redisMetaVisitor) {
        this.redisMetaVisitor = redisMetaVisitor;
    }

    @Override
    public void accept(ShardMeta shardMeta) {
        for(RedisMeta redisMeta : shardMeta.getRedises()) {
            redisMetaVisitor.accept(redisMeta);
        }
    }
}
