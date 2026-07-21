package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;

import java.util.List;

/**
 * @author TB
 * @date 2026/7/14 11:18
 */
public class DefaultRawCommand extends AbstractCommand<Boolean> implements RedisOpDataCommand<Boolean>{

    final AsyncRedisClient client;

    final Object resource;

    final List<byte[][]> rawArgs;

    public DefaultRawCommand(AsyncRedisClient client, Object resource, List<byte[][]> rawArgs) {
        this.client = client;
        this.resource = resource;
        this.rawArgs = rawArgs;
    }


    @Override
    protected void doExecute() throws Throwable {
        long startTime = System.nanoTime();

        client
                .writeMulti(resource, 0, rawArgs.toArray())
                .addListener(f -> {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("[command] write key {} end, total time {}", redisOp() instanceof RedisSingleKeyOp ? ((RedisSingleKeyOp) redisOp()).getKey() : (redisOp() instanceof RedisMultiKeyOp ? keys() : "none"), System.nanoTime() - startTime);
                    }
                    if (f.isSuccess()) {
                        future().setSuccess(true);
                    } else {
                        future().setFailure(f.cause());
                    }
                });
    }

    @Override
    protected void doReset() {

    }

    @Override
    public RedisOp redisOp() {
        return null;
    }
}
