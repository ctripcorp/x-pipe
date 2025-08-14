package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
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
import java.util.List;
import java.util.function.Consumer;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultDcMetaChangeManager extends AbstractStartStoppable implements DcMetaChangeManager, MetaComparatorVisitor<ClusterMeta> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDcMetaChangeManager.class);

    private DcMeta current;

    private HealthCheckInstanceManager instanceManager;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();
    
    private HealthCheckEndpointFactory healthCheckEndpointFactory;

    private MetaCache metaCache;

    private final String dcId;

    private final List<ClusterMeta> clustersToDelete = new ArrayList<>();
    private final List<ClusterMeta> clustersToAdd = new ArrayList<>();
    private final List<RedisMeta> redisListToDelete = new ArrayList<>();
    private final List<RedisMeta> redisListToAdd = new ArrayList<>();

    public DefaultDcMetaChangeManager(String dcId, HealthCheckInstanceManager instanceManager,
                                      HealthCheckEndpointFactory healthCheckEndpointFactory,
                                      MetaCache metaCache) {
        this.dcId = dcId;
        this.instanceManager = instanceManager;
        this.healthCheckEndpointFactory = healthCheckEndpointFactory;
        this.metaCache = metaCache;
    }

    @Override
    public void compare(DcMeta future) {
        // init
        if(current == null) {
            healthCheckEndpointFactory.updateRoutes();
            current = future;
            return;
        }

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

        boolean result = false;
        if (dcClusterIsMasterType(clusterType, cluster))
            result = clusterDcIsCurrentDc(cluster);
        if (hasSingleActiveDc(clusterType))
            result = result || cluster.getActiveDc().equalsIgnoreCase(currentDcId);
        if (hasMultipleActiveDcs(clusterType)) {
            if (StringUtil.isEmpty(cluster.getDcs())) return false;
            String[] dcs = cluster.getDcs().toLowerCase().split("\\s*,\\s*");
            result = result || Arrays.asList(dcs).contains(currentDcId.toLowerCase());
        }
        result = result || isOneWayClusterActiveDcCrossRegionAndCurrentDc(cluster);

        return result;
    }

    public boolean isOneWayClusterActiveDcCrossRegionAndCurrentDc(ClusterMeta cluster) {
        ClusterType clusterType = ClusterType.lookup(cluster.getType());
        return clusterType == ClusterType.ONE_WAY && isClusterActiveDcCrossRegion(cluster) && clusterDcIsCurrentDc(cluster);
    }

    private boolean clusterDcIsCurrentDc(ClusterMeta clusterMeta) {
        return clusterMeta.parent().getId().equalsIgnoreCase(currentDcId);
    }

    private boolean dcClusterIsMasterType(ClusterType clusterType, ClusterMeta clusterMeta) {
        if (!StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
            ClusterType azGroupType = ClusterType.lookup(clusterMeta.getAzGroupType());
            return clusterType == ClusterType.ONE_WAY && azGroupType == ClusterType.SINGLE_DC;
        }

        return false;
    }

    private boolean hasSingleActiveDc(ClusterType clusterType) {
        return clusterType.supportSingleActiveDC() || clusterType.isCrossDc();
    }

    private boolean hasMultipleActiveDcs(ClusterType clusterType) {
        return clusterType.supportMultiActiveDC() && !clusterType.isCrossDc();
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
        logger.info("[stop] {}", current.getId());
        for(ClusterMeta cluster : current.getClusters().values()) {
            visitRemoved(cluster);
        }
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
        instanceManager.getOrCreateRedisInstanceForPsubPingAction(added);
    }

}
