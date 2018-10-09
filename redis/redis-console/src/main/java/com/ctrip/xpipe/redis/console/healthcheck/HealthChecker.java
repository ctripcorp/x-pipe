package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface HealthChecker extends Lifecycle {
    public final static String ENABLED = "redis.health.check.enabled";
}
