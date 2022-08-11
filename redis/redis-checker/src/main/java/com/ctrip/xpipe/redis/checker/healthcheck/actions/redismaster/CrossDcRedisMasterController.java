package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.CrossDcSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.springframework.stereotype.Component;

@Component
public class CrossDcRedisMasterController implements RedisMasterController, CrossDcSupport {

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return true;
    }

}
