package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:13 PM
 */
public class DefaultApplierRedisCommand extends AbstractCommand implements ApplierRedisCommand {

    final AsyncRedisClient client;
    final RedisOp redisOp;

    public DefaultApplierRedisCommand(AsyncRedisClient client, RedisOp redisOp) {

        this.client = client;
        this.redisOp = redisOp;
    }

    @Override
    protected void doExecute() throws Throwable {

        //use async redis client
    }

    @Override
    protected void doReset() {

    }

    @Override
    public RedisOp redisOp() {
        return redisOp;
    }
}
