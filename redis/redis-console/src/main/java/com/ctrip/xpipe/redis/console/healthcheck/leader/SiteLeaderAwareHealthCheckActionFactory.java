package com.ctrip.xpipe.redis.console.healthcheck.leader;

import com.ctrip.xpipe.api.cluster.LeaderAware;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface SiteLeaderAwareHealthCheckActionFactory<V extends HealthCheckInstance> extends
        HealthCheckActionFactory<SiteLeaderAwareHealthCheckAction, V>, LeaderAware {

    SiteLeaderAwareHealthCheckAction create(V instance);

    void destroy(SiteLeaderAwareHealthCheckAction action);

    Class<? extends SiteLeaderAwareHealthCheckAction> support();
}
