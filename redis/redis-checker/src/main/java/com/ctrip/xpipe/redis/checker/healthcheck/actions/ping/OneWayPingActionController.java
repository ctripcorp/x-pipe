package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OneWayPingActionController implements PingActionController, OneWaySupport {

    @Autowired
    private MetaCache metaCache;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        if (metaCache.isCrossRegion(currentDcId, info.getActiveDc()) && currentDcId.equalsIgnoreCase(info.getDcId())) return true;
        boolean isCurrentDc = currentDcId.equalsIgnoreCase(info.getActiveDc());
        String azGroupType = info.getAzGroupType();
        if (StringUtil.isEmpty(azGroupType) || ClusterType.lookup(azGroupType) != ClusterType.SINGLE_DC) {
            return isCurrentDc;
        } else {
            return !isCurrentDc;
        }
    }

}
