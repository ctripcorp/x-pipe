package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface HealthCheckInstanceFactory {

    RedisHealthCheckInstance create(RedisMeta redisMeta);

    ClusterHealthCheckInstance create(ClusterMeta clusterMeta);

    void remove(RedisHealthCheckInstance instance);

    void remove(ClusterHealthCheckInstance instance);

    RedisHealthCheckInstance getOrCreateRedisInstanceForPsubPingAction(RedisMeta redis);
}
