package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultDcMetaChangeManager extends AbstractStartStoppable implements DcMetaChangeManager, MetaComparatorVisitor<ClusterMeta> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDcMetaChangeManager.class);

    private DcMeta current;

    private final HealthCheckInstanceManager instanceManager;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    private final HealthCheckEndpointFactory healthCheckEndpointFactory;

    private final MetaCache metaCache;

    private final String dcId;

    private final KeeperCheckSelector keeperSelector;

    private final KeeperCapabilityCache keeperCapabilityCache;

    private final List<ClusterMeta> clustersToDelete = new ArrayList<>();
    private final List<ClusterMeta> clustersToAdd = new ArrayList<>();
    private final List<RedisMeta> redisListToDelete = new ArrayList<>();
    private final List<RedisMeta> redisListToAdd = new ArrayList<>();

    public DefaultDcMetaChangeManager(String dcId, HealthCheckInstanceManager instanceManager,
                                      HealthCheckEndpointFactory healthCheckEndpointFactory,
                                      MetaCache metaCache) {
        this(dcId, instanceManager, healthCheckEndpointFactory, metaCache, null, null);
    }

    public DefaultDcMetaChangeManager(String dcId, HealthCheckInstanceManager instanceManager,
                                      HealthCheckEndpointFactory healthCheckEndpointFactory,
                                      MetaCache metaCache, KeeperCheckSelector keeperSelector,
                                      KeeperCapabilityCache keeperCapabilityCache) {
        this.dcId = dcId;
        this.instanceManager = instanceManager;
        this.healthCheckEndpointFactory = healthCheckEndpointFactory;
        this.metaCache = metaCache;
        this.keeperSelector = keeperSelector;
        this.keeperCapabilityCache = keeperCapabilityCache;
    }

    @Override
    public void compare(DcMeta future) {
        // init
        if(current == null) {
            healthCheckEndpointFactory.updateRoutes();
            current = future;
            reconcileKeepers(future);
            return;
        }

        try {
            // normal logic
            DcMetaComparator comparator = DcMetaComparator.buildComparator(current, future);
            DcRouteMetaComparator dcRouteMetaComparator = new DcRouteMetaComparator(current, future, Route.TAG_CONSOLE);
            dcRouteMetaComparator.compare();
            //change routes
            if(!dcRouteMetaComparator.getAdded().isEmpty()
                    || !dcRouteMetaComparator.getMofified().isEmpty()
                    || !dcRouteMetaComparator.getRemoved().isEmpty()) {
                healthCheckEndpointFactory.updateRoutes();
            }

            comparator.accept(this);
            removeAndAdd();
            clearUp();
            this.current = future;
        } finally {
            reconcileKeepers(future);
        }
    }

    private void reconcileKeepers(DcMeta future) {
        if (keeperSelector == null || keeperCapabilityCache == null) {
            return;
        }

        Map<HostPort, KeeperMeta> expected = new HashMap<>();
        for (KeeperMeta keeper : keeperSelector.select(future)) {
            expected.put(new HostPort(keeper.getIp(), keeper.getPort()), keeper);
        }

        Map<HostPort, KeeperHealthCheckInstance> actual = new HashMap<>();
        for (KeeperHealthCheckInstance instance : instanceManager.getKeeperInstancesByDc(dcId)) {
            HostPort address = getKeeperAddress(instance);
            if (address != null) {
                actual.put(address, instance);
            }
        }

        for (HostPort address : actual.keySet()) {
            if (!expected.containsKey(address)) {
                try {
                    instanceManager.removeKeeper(address);
                } catch (Throwable throwable) {
                    logger.error("[reconcileKeepers][remove] dc={}, keeper={}", dcId, address, throwable);
                } finally {
                    keeperCapabilityCache.invalidate(address);
                }
            }
        }
        for (Map.Entry<HostPort, KeeperMeta> entry : expected.entrySet()) {
            if (!actual.containsKey(entry.getKey())) {
                instanceManager.getOrCreate(entry.getValue());
            }
        }
    }

    private HostPort getKeeperAddress(KeeperHealthCheckInstance instance) {
        KeeperInstanceInfo info = instance == null ? null : instance.getCheckInfo();
        return info == null ? null : info.getHostPort();
    }

    private void removeAllKeepers() {
        if (keeperCapabilityCache == null) {
            return;
        }
        for (KeeperHealthCheckInstance instance : instanceManager.getKeeperInstancesByDc(dcId)) {
            HostPort address = getKeeperAddress(instance);
            if (address == null) {
                continue;
            }
            try {
                instanceManager.removeKeeper(address);
            } catch (Throwable throwable) {
                logger.error("[removeAllKeepers] dc={}, keeper={}", dcId, address, throwable);
            } finally {
                keeperCapabilityCache.invalidate(address);
            }
        }
        keeperCapabilityCache.invalidateDc(dcId);
    }

    private void removeAndAdd() {
        this.redisListToDelete.forEach(this::removeRedis);
        this.redisListToDelete.forEach(this::removeRedisOnlyForPingAction);
        this.clustersToDelete.forEach(this::removeCluster);

        this.clustersToAdd.forEach(this::addCluster);
        this.redisListToAdd.forEach(this::addRedis);
        this.redisListToAdd.forEach(this::addRedisOnlyForPingAction);
    }

    private void clearUp() {
        clustersToAdd.clear();
        clustersToDelete.clear();
        redisListToAdd.clear();
        redisListToDelete.clear();
    }

    private void removeCluster(ClusterMeta removed) {
        if (dcId.equalsIgnoreCase(currentDcId)) {
            logger.info("[removeCluster][{}][{}] remove dc current dc, remove cluster health check", dcId, removed.getId());
            instanceManager.remove(removed.getId());
        }

        logger.info("[removeCluster][{}][{}] remove health check", dcId, removed.getId());
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(removeConsumer)));
        ClusterMetaCrossRegionVisitor clusterMetaPingActionVisitor = new ClusterMetaCrossRegionVisitor(new ShardMetaCrossRegionVisitor(new RedisMetaVisitor(removePingActionConsumer)));
        clusterMetaVisitor.accept(removed);
        clusterMetaPingActionVisitor.accept(removed);
    }

    private void addCluster(ClusterMeta added) {
        if (isInterestedInCluster(added)) {
            logger.info("[addCluster][{}][{}] add health check", dcId, added.getId());
            instanceManager.getOrCreate(added);
            if (isOneWayClusterActiveDcCrossRegionAndCurrentDc(added)) {
                ClusterMetaCrossRegionVisitor clusterMetaPingActionVisitor = new ClusterMetaCrossRegionVisitor(new ShardMetaCrossRegionVisitor(new RedisMetaVisitor(addPingActionConsumer)));
                clusterMetaPingActionVisitor.accept(added);
            } else {
                ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
                clusterMetaVisitor.accept(added);
            }
        }
    }

    private void removeRedis(RedisMeta removed) {
        if (null != instanceManager.remove(new HostPort(removed.getIp(), removed.getPort()))) {
            logger.info("[removeRedis][{}:{}] {}", removed.getIp(), removed.getPort(), removed);
        }
    }

    private void addRedis(RedisMeta added) {
        if (!isInterestedInCluster(added.parent().parent()) || isOneWayClusterActiveDcCrossRegionAndCurrentDc(added.parent().parent())) {
            return;
        }
        logger.info("[addRedis][{}:{}] {}", added.getIp(), added.getPort(), added);
        instanceManager.getOrCreate(added);
    }

    @Override
    public void visitAdded(ClusterMeta added) {
        logger.debug("[visitAdded][{}][{}]", dcId, added.getId());
        this.clustersToAdd.add(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) comparator;
        if (comparator.isConfigChange()) {
            this.clustersToDelete.add(clusterMetaComparator.getCurrent());
            this.clustersToAdd.add(clusterMetaComparator.getFuture());
        } else {
            ClusterMetaComparatorCollector clusterMetaComparatorCollector = new ClusterMetaComparatorCollector();
            clusterMetaComparator.accept(clusterMetaComparatorCollector);
            Pair<List<RedisMeta>, List<RedisMeta>> modifiedRedises = clusterMetaComparatorCollector.collect();
            this.redisListToDelete.addAll(modifiedRedises.getKey());
            this.redisListToAdd.addAll(modifiedRedises.getValue());
        }
    }


    @Override
    public void visitRemoved(ClusterMeta removed) {
        logger.debug("[visitRemoved][{}][{}]", dcId, removed.getId());
        this.clustersToDelete.add(removed);
    }

    protected boolean isInterestedInCluster(ClusterMeta cluster) {
        ClusterType clusterType = ClusterType.lookup(cluster.getType());

        if (clusterType.supportSingleActiveDC() || clusterType.isCrossDc()) {
            return cluster.getActiveDc().equalsIgnoreCase(currentDcId) || isOneWayClusterActiveDcCrossRegionAndCurrentDc(cluster);
        }

        if (clusterType.supportMultiActiveDC()) {
            if (StringUtil.isEmpty(cluster.getDcs())) return false;
            String[] dcs = cluster.getDcs().toLowerCase().split("\\s*,\\s*");
            return Arrays.asList(dcs).contains(currentDcId.toLowerCase());
        }

        return true;
    }

    public boolean isOneWayClusterActiveDcCrossRegionAndCurrentDc(ClusterMeta cluster) {
        ClusterType clusterType = ClusterType.lookup(cluster.getType());
        return clusterType == ClusterType.ONE_WAY && isClusterActiveDcCrossRegion(cluster) && clusterDcIsCurrentDc(cluster);
    }

    private boolean clusterDcIsCurrentDc(ClusterMeta clusterMeta) {
        return clusterMeta.parent().getId().equalsIgnoreCase(currentDcId);
    }

    private boolean isClusterActiveDcCrossRegion(ClusterMeta clusterMeta) {
        if (clusterMeta.getActiveDc() == null) {
            logger.error("[DefaultDcMetaChangeManager][isClusterActiveDcCrossRegion]cluster has no active dc, clusterMeta:{}", clusterMeta);
            return false;
        }
        return metaCache.isCrossRegion(currentDcId, clusterMeta.getActiveDc());
    }

    private Consumer<RedisMeta> removeConsumer = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            removeRedis(redisMeta);
        }
    };

    private Consumer<RedisMeta> addConsumer = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            addRedis(redisMeta);
        }
    };

    private Consumer<RedisMeta> removePingActionConsumer = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            removeRedisOnlyForPingAction(redisMeta);
        }
    };

    private Consumer<RedisMeta> addPingActionConsumer = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            addRedisOnlyForPingAction(redisMeta);
        }
    };

    @Override
    protected void doStart() {
        if (current == null) {
            logger.error("[start] cannot start without a DcMeta");
            return;
        }
        logger.info("[start] {}", current.getId());
        for (ClusterMeta cluster : current.getClusters().values()) {
            visitAdded(cluster);
        }
    }

    @Override
    protected void doStop() {
        if (current != null) {
            logger.info("[stop] {}", current.getId());
            for(ClusterMeta cluster : current.getClusters().values()) {
                visitRemoved(cluster);
            }
        }
        removeAllKeepers();
    }

    private void removeRedisOnlyForPingAction(RedisMeta removed) {
        if (null != instanceManager.removeRedisInstanceForPingAction(new HostPort(removed.getIp(), removed.getPort()))) {
            logger.info("[removeRedisOnlyForPingAction][{}:{}] {}", removed.getIp(), removed.getPort(), removed);
        }
    }

    private void addRedisOnlyForPingAction(RedisMeta added) {
        if (!isOneWayClusterActiveDcCrossRegionAndCurrentDc(added.parent().parent())) {
            return;
        }
        logger.info("[addRedisOnlyForPingAction][{}:{}] {}", added.getIp(), added.getPort(), added);
        instanceManager.getOrCreateRedisInstanceForInfoReplIdAction(added);
    }

}
