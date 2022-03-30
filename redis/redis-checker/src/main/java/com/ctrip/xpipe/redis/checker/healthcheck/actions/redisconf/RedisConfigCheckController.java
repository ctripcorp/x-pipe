package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RedisConfigCheckController extends CurrentDcCheckController implements BiDirectionSupport, OneWaySupport {
    @Autowired
    public RedisConfigCheckController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return super.shouldCheck(instance);
    }
}
