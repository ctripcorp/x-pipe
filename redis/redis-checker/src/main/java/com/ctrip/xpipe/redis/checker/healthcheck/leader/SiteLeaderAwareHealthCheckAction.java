package com.ctrip.xpipe.redis.checker.healthcheck.leader;

import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface SiteLeaderAwareHealthCheckAction<T extends HealthCheckInstance>  extends HealthCheckAction<T> {
}
