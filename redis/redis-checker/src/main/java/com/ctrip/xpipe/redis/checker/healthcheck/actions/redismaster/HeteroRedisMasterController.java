package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.HeteroSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class HeteroRedisMasterController implements RedisMasterController, HeteroSupport {

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
//        todo: in active dc or in master type dcGroup
        return false;
    }

}
