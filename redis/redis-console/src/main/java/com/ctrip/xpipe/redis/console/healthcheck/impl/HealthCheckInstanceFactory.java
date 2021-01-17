package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
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

}
