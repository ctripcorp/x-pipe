package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class ShardMetaVisitor implements MetaVisitor<ShardMeta> {

    private RedisMetaVisitor redisMetaVisitor;

    public ShardMetaVisitor(RedisMetaVisitor redisMetaVisitor) {
        this.redisMetaVisitor = redisMetaVisitor;
    }

    @Override
    public void accept(ShardMeta shardMeta) {
        for(RedisMeta redisMeta : shardMeta.getRedises()) {
            redisMetaVisitor.accept(redisMeta);
        }
    }
}
