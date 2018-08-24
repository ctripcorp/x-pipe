package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public interface HealthCheckContextFactory {
    HealthCheckContext create(RedisHealthCheckInstance instance, RedisMeta redis);
}
