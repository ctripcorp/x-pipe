package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface HealthCheckInstanceManager {

    RedisHealthCheckInstance getOrCreate(RedisMeta redis);

    RedisHealthCheckInstance findRedisHealthCheckInstance(HostPort hostPort);

    void remove(HostPort hostPort);

    List<RedisHealthCheckInstance> getAllRedisInstance();
}
