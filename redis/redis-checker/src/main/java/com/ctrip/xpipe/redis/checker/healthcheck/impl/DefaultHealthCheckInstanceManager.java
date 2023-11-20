package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultHealthCheckInstanceManager implements HealthCheckInstanceManager {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private ConcurrentMap<HostPort, RedisHealthCheckInstance> instances = Maps.newConcurrentMap();

    private ConcurrentMap<String, ClusterHealthCheckInstance> clusterHealthCheckerInstances = Maps.newConcurrentMap();

    private ConcurrentMap<HostPort, KeeperHealthCheckInstance> keeperInstances = Maps.newConcurrentMap();

    private ConcurrentMap<HostPort, RedisHealthCheckInstance> redisInstanceForAssignedAction = Maps.newConcurrentMap();

    @Autowired
    private HealthCheckInstanceFactory instanceFactory;

    @Override
    public RedisHealthCheckInstance getOrCreate(RedisMeta redis) {
        try {
            HostPort key = new HostPort(redis.getIp(), redis.getPort());
            return MapUtils.getOrCreate(instances, key, () -> instanceFactory.create(redis));
        } catch (Throwable th) {
            logger.error("getOrCreate health check instance:{}:{}", redis.getIp(), redis.getPort());
        }
        return null;
    }

    @Override
    public RedisHealthCheckInstance getOrCreateRedisInstanceForAssignedAction(RedisMeta redis) {
        try {
            HostPort key = new HostPort(redis.getIp(), redis.getPort());
            return MapUtils.getOrCreate(redisInstanceForAssignedAction, key,
                    () -> instanceFactory.createRedisInstanceForAssignedAction(redis));
        } catch (Throwable th) {
            logger.error("getOrCreate health check redis instance:{}:{}", redis.getIp(), redis.getPort());
        }
        return null;
    }

    @Override
    public KeeperHealthCheckInstance getOrCreate(KeeperMeta keeper) {
        try {
            HostPort key = new HostPort(keeper.getIp(), keeper.getPort());
            return MapUtils.getOrCreate(keeperInstances, key, () -> instanceFactory.create(keeper));
        } catch (Throwable th) {
            logger.error("getOrCreate health check keeper instance:{}:{}", keeper.getIp(), keeper.getPort());
        }
        return null;
    }

    @Override
    public ClusterHealthCheckInstance getOrCreate(ClusterMeta cluster) {
        try {
            String key = cluster.getId().toLowerCase();
            return MapUtils.getOrCreate(clusterHealthCheckerInstances, key, () -> instanceFactory.create(cluster));
        } catch (Throwable th) {
            logger.error("getOrCreate health check cluster:{}", cluster.getId());
        }
        return null;
    }

    @Override
    public RedisHealthCheckInstance findRedisHealthCheckInstance(HostPort hostPort) {
        return instances.get(hostPort);
    }

    @Override
    public RedisHealthCheckInstance findRedisInstanceForAssignedAction(HostPort hostPort) {
        return redisInstanceForAssignedAction.get(hostPort);
    }

    @Override
    public KeeperHealthCheckInstance findKeeperHealthCheckInstance(HostPort hostPort) {
        return keeperInstances.get(hostPort);
    }

    @Override
    public ClusterHealthCheckInstance findClusterHealthCheckInstance(String clusterId) {
        if (StringUtil.isEmpty(clusterId)) return null;
        return clusterHealthCheckerInstances.get(clusterId.toLowerCase());
    }

    @Override
    public RedisHealthCheckInstance remove(HostPort hostPort) {
        RedisHealthCheckInstance instance = instances.remove(hostPort);
        if (null != instance) instanceFactory.remove(instance);
        return instance;
    }

    @Override
    public KeeperHealthCheckInstance removeKeeper(HostPort hostPort) {
        KeeperHealthCheckInstance instance = keeperInstances.remove(hostPort);
        if (null != instance) instanceFactory.remove(instance);
        return instance;
    }

    @Override
    public RedisHealthCheckInstance removeRedisInstanceForAssignedAction(HostPort hostPort) {
        RedisHealthCheckInstance instance = redisInstanceForAssignedAction.remove(hostPort);
        if (null != instance) instanceFactory.remove(instance);
        return instance;
    }


    @Override
    public ClusterHealthCheckInstance remove(String cluster) {
        ClusterHealthCheckInstance instance = clusterHealthCheckerInstances.remove(cluster.toLowerCase());
        if (null != instance) instanceFactory.remove(instance);
        return instance;
    }

    @Override
    public List<RedisHealthCheckInstance> getAllRedisInstance() {
        return Lists.newLinkedList(instances.values());
    }

    @Override
    public List<KeeperHealthCheckInstance> getAllKeeperInstance() {
        return Lists.newLinkedList(keeperInstances.values());
    }

    @Override
    public List<RedisHealthCheckInstance> getAllRedisInstanceForAssignedAction() {
        return Lists.newLinkedList(redisInstanceForAssignedAction.values());
    }

    @Override
    public List<ClusterHealthCheckInstance> getAllClusterInstance() {
        return Lists.newLinkedList(clusterHealthCheckerInstances.values());
    }

}
