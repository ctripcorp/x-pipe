package com.ctrip.xpipe.redis.console.healthcheck.crossdc;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface SiteLeaderAwareHealthCheckActionFactory extends
        HealthCheckActionFactory<SiteLeaderAwareHealthCheckAction>, LeaderAware {

    SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance);

    void destroy(SiteLeaderAwareHealthCheckAction action);

    Class<? extends SiteLeaderAwareHealthCheckAction> support();
}
