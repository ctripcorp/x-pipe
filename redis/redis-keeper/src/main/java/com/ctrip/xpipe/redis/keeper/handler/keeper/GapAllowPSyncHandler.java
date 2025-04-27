package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.RedisSlave;

public class GapAllowPSyncHandler extends GapAllowSyncHandler {

    protected SyncRequest parseRequest(final String[] args, RedisSlave redisSlave) {
        if (args.length < 2) {
            redisSlave.sendMessage(new RedisErrorParser(new RedisError("Invalid argument count")).format());
            return null;
        }

        String replId = args[0];
        Long offset = Long.valueOf(args[1]);
        return SyncRequest.psync(replId, offset);
    }

    @Override
    public String[] getCommands() {
        return new String[] { "psync" };
    }
}
