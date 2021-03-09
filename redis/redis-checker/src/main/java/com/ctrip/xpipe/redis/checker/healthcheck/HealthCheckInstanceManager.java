package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface HealthCheckInstanceManager {

    RedisHealthCheckInstance getOrCreate(RedisMeta redis);

    ClusterHealthCheckInstance getOrCreate(ClusterMeta cluster);

    RedisHealthCheckInstance findRedisHealthCheckInstance(HostPort hostPort);

    ClusterHealthCheckInstance findClusterHealthCheckInstance(String clusterId);

    void remove(HostPort hostPort);

    void remove(String cluster);

    List<RedisHealthCheckInstance> getAllRedisInstance();

    List<ClusterHealthCheckInstance> getAllClusterInstance();

}
