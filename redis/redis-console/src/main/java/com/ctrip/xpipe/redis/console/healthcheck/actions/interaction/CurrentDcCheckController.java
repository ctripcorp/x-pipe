package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionController;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class CurrentDcCheckController implements HealthCheckActionController {

    protected static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        String dcId = instance.getRedisInstanceInfo().getDcId();
        return currentDcId.equalsIgnoreCase(dcId);
    }

}
