package com.ctrip.xpipe.redis.console.healthcheck.crossdc;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface CrossDcLeaderAwareHealthCheckActionFactory extends
        HealthCheckActionFactory<CrossDcLeaderAwareHealthCheckAction>, CrossDcLeaderAware {

    CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance);

    void destroy(CrossDcLeaderAwareHealthCheckAction action);

    Class<? extends CrossDcLeaderAwareHealthCheckAction> support();
}
