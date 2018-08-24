package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.function.Consumer;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class RedisMetaVisitor implements MetaVisitor<RedisMeta> {

    private Consumer<RedisMeta> consumer;

    public RedisMetaVisitor(Consumer<RedisMeta> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void accept(RedisMeta redisMeta) {
        consumer.accept(redisMeta);
    }
}
