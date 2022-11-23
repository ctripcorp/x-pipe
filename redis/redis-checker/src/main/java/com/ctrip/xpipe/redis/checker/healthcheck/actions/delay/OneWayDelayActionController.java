package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import org.springframework.stereotype.Component;

@Component
public class OneWayDelayActionController implements DelayActionController, OneWaySupport {

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo instanceInfo = instance.getCheckInfo();
        return !currentDcId.equalsIgnoreCase(instanceInfo.getActiveDc())
                || DcGroupType.isNullOrDrMaster(instanceInfo.getDcGroupType())
                || instanceInfo.isMaster();
    }

}
