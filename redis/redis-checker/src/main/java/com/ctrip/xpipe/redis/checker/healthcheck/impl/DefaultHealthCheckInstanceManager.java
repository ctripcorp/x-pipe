package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
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

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckInstanceManager.class);

    private ConcurrentMap<HostPort, RedisHealthCheckInstance> instances = Maps.newConcurrentMap();

    private ConcurrentMap<String, ClusterHealthCheckInstance> clusterHealthCheckerInstances = Maps.newConcurrentMap();

    @Autowired
    private HealthCheckInstanceFactory instanceFactory;

    @Override
    public RedisHealthCheckInstance getOrCreate(RedisMeta redis) {
        HostPort key = new HostPort(redis.getIp(), redis.getPort());
        return MapUtils.getOrCreate(instances, key, () -> instanceFactory.create(redis));
    }

    @Override
    public ClusterHealthCheckInstance getOrCreate(ClusterMeta cluster) {
        String key = cluster.getId().toLowerCase();
        return MapUtils.getOrCreate(clusterHealthCheckerInstances, key, () -> instanceFactory.create(cluster));
    }

    @Override
    public RedisHealthCheckInstance findRedisHealthCheckInstance(HostPort hostPort) {
        return instances.get(hostPort);
    }

    @Override
    public ClusterHealthCheckInstance findClusterHealthCheckInstance(String clusterId) {
        if (StringUtil.isEmpty(clusterId)) return null;
        return clusterHealthCheckerInstances.get(clusterId.toLowerCase());
    }

    @Override
    public void remove(HostPort hostPort) {
        RedisHealthCheckInstance instance = instances.remove(hostPort);
        stopCheck(instance);
    }

    @Override
    public void remove(String cluster) {
        ClusterHealthCheckInstance instance = clusterHealthCheckerInstances.remove(cluster.toLowerCase());
        stopCheck(instance);
    }

    @Override
    public List<RedisHealthCheckInstance> getAllRedisInstance() {
        return Lists.newLinkedList(instances.values());
    }

    @Override
    public List<ClusterHealthCheckInstance> getAllClusterInstance() {
        return Lists.newLinkedList(clusterHealthCheckerInstances.values());
    }

    private void stopCheck(HealthCheckInstance instance) {
        try {
            LifecycleHelper.stopIfPossible(instance);
        } catch (Exception e) {
            logger.error("[stopCheck]", e);
        }
    }

}
