package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

/**
 * @author Slight
 * <p>
 * Jun 08, 2022 15:49
 */
public class DefaultMultiCommand extends AbstractCommand<Boolean> implements RedisOpCommand<Boolean> {

    final AsyncRedisClient client;

    final RedisOp redisOp;

    public DefaultMultiCommand(AsyncRedisClient client, RedisOp redisOp) {
        this.client = client;
        this.redisOp = redisOp;
    }


    @Override
    protected void doExecute() throws Throwable {

        client.multi().addListener(f->{
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
        return redisOp;
    }
}
