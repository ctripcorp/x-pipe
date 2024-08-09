package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OneWayPsubActionController implements PsubActionController, OneWaySupport {

    @Autowired
    private MetaCache metaCache;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Override
    public boolean shouldCheck(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        return metaCache.isCrossRegion(currentDcId, info.getActiveDc()) && currentDcId.equalsIgnoreCase(info.getDcId());
    }
}
