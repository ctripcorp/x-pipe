package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ActiveDcRedisMasterController implements RedisMasterController, OneWaySupport {

    private static Logger logger = LoggerFactory.getLogger(ActiveDcRedisMasterController.class);

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        if(!info.isInActiveDc()) {
            logger.debug("[doTask] not in backup dc: {}", info);
            return false;
        }

        return true;
    }
}
