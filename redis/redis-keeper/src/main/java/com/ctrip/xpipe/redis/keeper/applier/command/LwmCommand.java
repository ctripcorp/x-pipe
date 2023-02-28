package com.ctrip.xpipe.redis.keeper.applier.command;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;

public class LwmCommand extends DefaultBroadcastCommand {

    public LwmCommand(AsyncRedisClient client, RedisOp redisOp) {
        super(client, redisOp);
    }

    @Override
    public boolean needGuaranteeSuccess() {
        return false;
    }

    @Override
    public RedisOpCommandType type() {
        return RedisOpCommandType.NONE_KEY;
    }
}
