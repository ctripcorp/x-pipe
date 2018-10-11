package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface RedisHealthCheckInstanceFactory {
    RedisHealthCheckInstance create(RedisMeta redisMeta);
}
