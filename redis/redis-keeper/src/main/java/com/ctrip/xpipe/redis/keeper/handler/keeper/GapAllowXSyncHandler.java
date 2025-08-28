package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.keeper.RedisSlave;

public class GapAllowXSyncHandler extends GapAllowSyncHandler {

    protected SyncRequest parseRequest(final String[] args, RedisSlave redisSlave) {
        if (args.length < 2) {
            redisSlave.sendMessage(new RedisErrorParser(new RedisError("Invalid argument count")).format());
            return null;
        }

        String replId = args[0];
        String gtidset = args[1];
        int pos = 2;
        long maxGap = -1;
        while (pos < args.length) {
            String opt = args[pos];
            if (opt.equalsIgnoreCase("maxgap")) {
                if (args.length < pos + 2) {
                    redisSlave.sendMessage(new RedisErrorParser(new RedisError("Invalid maxgap")).format());
                    return null;
                }

                maxGap = Long.parseLong(args[pos + 1]);
                pos += 2;
            } else {
                redisSlave.sendMessage(new RedisErrorParser(new RedisError("Invalid option " + opt)).format());
                return null;
            }
        }

        return SyncRequest.xsync(replId, gtidset, maxGap);
    }

    @Override
    public String[] getCommands() {
        return new String[] { "xsync" };
    }

}
