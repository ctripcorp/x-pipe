package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface HealthCheckRedisInstanceFactory {
    RedisHealthCheckInstance create(RedisMeta redisMeta);
}
