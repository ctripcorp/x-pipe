package com.ctrip.xpipe.redis.console.healthcheck.leader;

import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface SiteLeaderAwareHealthCheckAction<T extends HealthCheckInstance>  extends HealthCheckAction<T> {
}
