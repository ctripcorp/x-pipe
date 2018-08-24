package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 03, 2018
 */
public interface HealthEventProcessor {

    void markDown(RedisHealthCheckInstance instance);

    void markUp(RedisHealthCheckInstance instance);
}
