package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.redis.operation.RedisKey;
import com.ctrip.xpipe.redis.core.redis.operation.RedisMultiKeyOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisSingleKeyOp;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Slight
 * <p>
 * Feb 26, 2022 3:13 PM
 */
public class DefaultDataCommand extends AbstractCommand<Boolean> implements RedisOpDataCommand<Boolean> {

    public static String ERR_GTID_COMMAND_EXECUTED = "ERR gtid command is executed";

    final AsyncRedisClient client;

    final Object resource;

    final RedisOp redisOp;

    public DefaultDataCommand(AsyncRedisClient client, RedisOp redisOp) {
        this(client, null, redisOp);
    }

    public DefaultDataCommand(AsyncRedisClient client, Object resource, RedisOp redisOp) {

        this.client = client;
        this.resource = resource;
        this.redisOp = redisOp;
    }

    @Override
    protected void doExecute() throws Throwable {

        Object rc = resource != null ? resource : client.select(key().get());
        Object[] rawArgs = redisOp.buildRawOpArgs();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[command] write key {} start", redisOp() instanceof RedisSingleKeyOp ? ((RedisSingleKeyOp) redisOp()).getKey() : (redisOp() instanceof RedisMultiKeyOp ? keys() : "none"));
        }

        long startTime = System.nanoTime();

        client
                .write(rc, rawArgs)
                .addListener(f -> {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("[command] write key {} end, total time {}", redisOp() instanceof RedisSingleKeyOp ? ((RedisSingleKeyOp) redisOp()).getKey() : (redisOp() instanceof RedisMultiKeyOp ? keys() : "none"), System.nanoTime() - startTime);
                    }
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
