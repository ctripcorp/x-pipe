package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
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

    private ConcurrentMap<HostPort, RedisHealthCheckInstance> redisInstanceForPingAction = Maps.newConcurrentMap();

    private static final String ALERT_TYPE = "HealthCheckInstance";

    @Autowired
    private HealthCheckInstanceFactory instanceFactory;

    @Override
    public RedisHealthCheckInstance getOrCreate(RedisMeta redis) {
        try {
            HostPort key = new HostPort(redis.getIp(), redis.getPort());
            return MapUtils.getOrCreate(instances, key, () -> instanceFactory.create(redis));
        } catch (Throwable th) {
            logger.error("getOrCreate health check instance:{}:{}", redis.getIp(), redis.getPort(), th);
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
            logger.error("getOrCreate health check redis instance:{}:{}", redis.getIp(), redis.getPort(), th);
        }
        return null;
    }

    @Override
    public RedisHealthCheckInstance getOrCreateRedisInstanceForPsubPingAction(RedisMeta redis) {
        try {
            HostPort key = new HostPort(redis.getIp(), redis.getPort());
            return MapUtils.getOrCreate(redisInstanceForPingAction, key,
                    () -> instanceFactory.getOrCreateRedisInstanceForPsubPingAction(redis));
        } catch (Throwable th) {
            logger.error("getOrCreate ping action health check redis instance:{}:{}", redis.getIp(), redis.getPort(), th);
        }
        return null;
    }

    @Override
    public KeeperHealthCheckInstance getOrCreate(KeeperMeta keeper) {
        try {
            HostPort key = new HostPort(keeper.getIp(), keeper.getPort());
            return MapUtils.getOrCreate(keeperInstances, key, () -> instanceFactory.create(keeper));
        } catch (Throwable th) {
            logger.error("getOrCreate health check keeper instance:{}:{}", keeper.getIp(), keeper.getPort(), th);
        }
        return null;
    }

    @Override
    public ClusterHealthCheckInstance getOrCreate(ClusterMeta cluster) {
        try {
            String key = cluster.getId().toLowerCase();
            return MapUtils.getOrCreate(clusterHealthCheckerInstances, key, () -> instanceFactory.create(cluster));
        } catch (Throwable th) {
            logger.error("getOrCreate health check cluster:{}", cluster.getId(), th);
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
    public RedisHealthCheckInstance findRedisInstanceForPsubPingAction(HostPort hostPort) {
        return redisInstanceForPingAction.get(hostPort);
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
    public RedisHealthCheckInstance removeRedisInstanceForPingAction(HostPort hostPort) {
        RedisHealthCheckInstance instance = redisInstanceForPingAction.remove(hostPort);
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

    public boolean checkInstancesMiss(XpipeMeta xpipeMeta) {
        if (null == xpipeMeta) return true;

        logger.debug("[checkInstancesMiss][begin]");
        Set<String> currentClusters = clusterHealthCheckerInstances.keySet();
        Set<HostPort> currentInstances = instances.keySet();
        Set<HostPort> currentPingInstances = redisInstanceForPingAction.keySet();

        Set<String> expectClusters = new HashSet<>();
        Set<HostPort> expectInstances = new HashSet<>();
        Set<HostPort> expectPingInstances = new HashSet<>();

        String currentDc = FoundationService.DEFAULT.getDataCenter();
        String currentZone = xpipeMeta.getDcs().get(currentDc).getZone();
        for (Map.Entry<String, DcMeta> entry: xpipeMeta.getDcs().entrySet()) {
            String dcId = entry.getKey();
            DcMeta dcMeta = entry.getValue();
            for (ClusterMeta clusterMeta: dcMeta.getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                String clusterId = clusterMeta.getId().toLowerCase();
                boolean addInstanceForPing = false;
                if (clusterType.equals(ClusterType.ONE_WAY)) {
                    String activeDc = clusterMeta.getActiveDc();
                    if (activeDc.equalsIgnoreCase(currentDc)) {
                        expectClusters.add(clusterId);
                    } else if (dcId.equalsIgnoreCase(currentDc) && !currentZone.equalsIgnoreCase(xpipeMeta.getDcs().get(activeDc).getZone())) {
                        addInstanceForPing = true;
                    } else {
                        continue;
                    }
                } else if (clusterType.supportSingleActiveDC() || clusterType.equals(ClusterType.CROSS_DC)) {
                    String activeDc = clusterMeta.getActiveDc();
                    if (activeDc.equalsIgnoreCase(currentDc)) {
                        expectClusters.add(clusterId);
                    } else {
                        continue;
                    }
                } else if (!expectClusters.contains(clusterId)) {
                    String[] dcs = clusterMeta.getDcs().split("\\s*,\\s*");
                    for (String dc : dcs) {
                        if (dc.equalsIgnoreCase(currentDc)) expectClusters.add(clusterId);
                    }

                    if (!expectClusters.contains(clusterId)) continue;
                }

                for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
                    for (RedisMeta redisMeta: shardMeta.getRedises()) {
                        if (addInstanceForPing) {
                            expectPingInstances.add(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                        } else {
                            expectInstances.add(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                        }
                    }
                }
            }
        }

        boolean noMissing = true;
        if (!currentClusters.equals(expectClusters)) {
            noMissing = false;
            logger.debug("[checkInstancesMiss][cluster][current] {}", currentClusters);
            logger.debug("[checkInstancesMiss][cluster][expect] {}", expectClusters);
            EventMonitor.DEFAULT.logEvent(ALERT_TYPE, "clusterMissing");
        }
        if (!currentInstances.equals(expectInstances)) {
            noMissing = false;
            logger.debug("[checkInstancesMiss][instance][current] {}", currentInstances);
            logger.debug("[checkInstancesMiss][instance][expect] {}", expectInstances);
            EventMonitor.DEFAULT.logEvent(ALERT_TYPE, "instanceMissing");
        }
        if (!currentPingInstances.equals(expectPingInstances)) {
            noMissing = false;
            logger.debug("[checkInstancesMiss][CrossRegionInstance][current] {}", currentPingInstances);
            logger.debug("[checkInstancesMiss][CrossRegionInstance][expect] {}", expectPingInstances);
            EventMonitor.DEFAULT.logEvent(ALERT_TYPE, "CrossRegionInstanceMissing");
        }
        if (noMissing) {
            EventMonitor.DEFAULT.logEvent(ALERT_TYPE, "noMissing");
        }

        return noMissing;
    }

}
