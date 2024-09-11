package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.stereotype.Component;

@Component
public class OneWayPingActionController implements PingActionController, OneWaySupport {

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        boolean isCurrentDc = currentDcId.equalsIgnoreCase(info.getActiveDc());
        String azGroupType = info.getAzGroupType();
        if (StringUtil.isEmpty(azGroupType) || ClusterType.lookup(azGroupType) != ClusterType.SINGLE_DC) {
            return isCurrentDc;
        } else {
            return !isCurrentDc;
        }
    }

}
