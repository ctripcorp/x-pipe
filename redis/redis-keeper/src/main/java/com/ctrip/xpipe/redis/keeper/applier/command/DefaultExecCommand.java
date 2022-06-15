package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

/**
 * @author Slight
 * <p>
 * Jun 08, 2022 15:50
 */
public class DefaultExecCommand extends AbstractCommand<Boolean> implements RedisOpCommand<Boolean> {

    public static String ERR_GTID_COMMAND_EXECUTED = "ERR gtid command is executed";

    final AsyncRedisClient client;

    final RedisOp redisOp;

    public DefaultExecCommand(AsyncRedisClient client, RedisOp redisOp) {

        this.client = client;
        this.redisOp = redisOp;
    }

    @Override
    protected void doExecute() throws Throwable {

        Object[] rawArgs = redisOp.buildRawOpArgs();

        client.exec(rawArgs).addListener(f->{
            if (f.isSuccess()) {
                future().setSuccess(true);
            } else {
                if (f.cause().getMessage().startsWith(ERR_GTID_COMMAND_EXECUTED)) {
                    future().setSuccess(true);
                } else {
                    future().setFailure(f.cause());
                }
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
