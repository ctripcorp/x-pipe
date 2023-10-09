package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.stereotype.Component;

@Component
public class OneWayDelayActionController implements DelayActionController, OneWaySupport {

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo instanceInfo = instance.getCheckInfo();
        String azGroupType = instanceInfo.getAzGroupType();
        if (StringUtil.isEmpty(azGroupType) || ClusterType.lookup(azGroupType) != ClusterType.SINGLE_DC) {
            return true;
        }
        return !currentDcId.equalsIgnoreCase(instanceInfo.getActiveDc()) || instanceInfo.isMaster();
    }

}
