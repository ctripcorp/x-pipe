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

    boolean needGuaranteeSuccess;

    public DefaultBroadcastCommand(AsyncRedisClient client, RedisOp redisOp) {
        // needGuaranteeSuccess default is true
        this(client, redisOp, true);
    }

    public DefaultBroadcastCommand(AsyncRedisClient client, RedisOp redisOp, boolean needGuaranteeSuccess) {
        this.client = client;
        this.redisOp = redisOp;
        this.needGuaranteeSuccess = needGuaranteeSuccess;
    }

    @Override
    protected void doExecute() throws Throwable {

        /* DONE: we should get masters only */
        Object[] resources = client.broadcast();
        Object[] rawArgs = redisOp.buildRawOpArgs();

        for (Object rc : resources) {

            client
                    .write(rc, rawArgs)
                    .addListener(f->{
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

    @Override
    public boolean needGuaranteeSuccess() {
        return needGuaranteeSuccess;
    }
}
