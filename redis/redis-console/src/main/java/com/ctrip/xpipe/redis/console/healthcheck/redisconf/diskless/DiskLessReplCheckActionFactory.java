package com.ctrip.xpipe.redis.console.healthcheck.redisconf.diskless;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.AbstractCDLAHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.redisconf.CrossDcLeaderAwareHealthCheckAction;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */

@Component
public class DiskLessReplCheckActionFactory extends AbstractCDLAHealthCheckActionFactory {

    @Override
    public CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        return new DiskLessReplCheckAction(scheduled, instance, executors, alertManager);
    }

    @Override
    public Class<? extends CrossDcLeaderAwareHealthCheckAction> support() {
        return DiskLessReplCheckAction.class;
    }
}
