package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 09:48
 */
public class DefaultBroadcastCommand extends AbstractCommand<Boolean> implements RedisOpCommand<Boolean> {

    final AsyncRedisClient client;

    final RedisOp redisOp;

    public DefaultBroadcastCommand(AsyncRedisClient client, RedisOp redisOp) {
        this.client = client;
        this.redisOp = redisOp;
    }

    @Override
    protected void doExecute() throws Throwable {

        /* DONE: we should get masters only */
        Object[] resources = client.broadcast();
        Object[] rawArgs = redisOp.buildRawOpArgs();

        for (Object rc : resources) {

            client
                    .write(rc, 0, rawArgs) // merge_start&merge_end&lwm do not distinguish dbNumber
                    .addListener(f -> {
                        try {
                            if (f.isSuccess()) {
                                future().setSuccess(true);
                            } else {
                                future().setFailure(f.cause());
                            }
                        } catch (IllegalStateException alreadyDone) {
                            //ignore
                        }
                    });
        }
    }

    @Override
    protected void doReset() {

    }

    @Override
    public RedisOp redisOp() {
        return redisOp;
    }

    @Override
    public RedisOpCommandType type() {
        return RedisOpCommandType.OTHER;
    }
}
