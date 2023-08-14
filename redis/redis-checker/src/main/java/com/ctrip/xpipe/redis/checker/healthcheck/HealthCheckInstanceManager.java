package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface HealthCheckInstanceManager {

    RedisHealthCheckInstance getOrCreate(RedisMeta redis);

    RedisHealthCheckInstance getOrCreateRedisInstanceOnlyForUsedMemory(RedisMeta redis);

    KeeperHealthCheckInstance getOrCreate(KeeperMeta keeper);

    ClusterHealthCheckInstance getOrCreate(ClusterMeta cluster);

    RedisHealthCheckInstance findRedisHealthCheckInstance(HostPort hostPort);

    KeeperHealthCheckInstance findKeeperHealthCheckInstance(HostPort hostPort);

    ClusterHealthCheckInstance findClusterHealthCheckInstance(String clusterId);

    RedisHealthCheckInstance remove(HostPort hostPort);

    KeeperHealthCheckInstance removeKeeper(HostPort hostPort);

    RedisHealthCheckInstance  removeRedisOnlyForUsedMemory(HostPort hostPort);

    ClusterHealthCheckInstance remove(String cluster);

    List<RedisHealthCheckInstance> getAllRedisInstance();

    List<KeeperHealthCheckInstance> getAllKeeperInstance();

    List<ClusterHealthCheckInstance> getAllClusterInstance();

}
