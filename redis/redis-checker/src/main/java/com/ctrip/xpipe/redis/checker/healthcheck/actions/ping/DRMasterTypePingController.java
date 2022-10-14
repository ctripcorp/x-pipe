package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.springframework.stereotype.Component;

@Component
public class DRMasterTypePingController implements PingActionController, OneWaySupport {

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        return currentDcId.equalsIgnoreCase(instance.getCheckInfo().getActiveDc()) && instance.getCheckInfo().getDcGroupType().isValue();
    }

}
