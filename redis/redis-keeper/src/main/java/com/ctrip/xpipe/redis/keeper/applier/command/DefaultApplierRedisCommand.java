package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:13 PM
 */
public class DefaultApplierRedisCommand extends AbstractCommand<Boolean> implements ApplierRedisCommand<Boolean> {

    final AsyncRedisClient client;

    final Object resource;

    final RedisOp redisOp;

    public DefaultApplierRedisCommand(AsyncRedisClient client, RedisOp redisOp) {
        this(client, null, redisOp);
    }

    public DefaultApplierRedisCommand(AsyncRedisClient client, Object resource, RedisOp redisOp) {

        this.client = client;
        this.resource = resource;
        this.redisOp = redisOp;
    }

    @Override
    protected void doExecute() throws Throwable {

        Object rc = resource != null ? resource : client.select(key().toString());
        Object[] rawArgs = redisOp.buildRawOpArgs().toArray();

        client
                .write(rc, rawArgs)
                .addListener(f->future().setSuccess(f.isSuccess()));
    }

    @Override
    protected void doReset() {

    }

    @Override
    public RedisOp redisOp() {
        return redisOp;
    }
}
