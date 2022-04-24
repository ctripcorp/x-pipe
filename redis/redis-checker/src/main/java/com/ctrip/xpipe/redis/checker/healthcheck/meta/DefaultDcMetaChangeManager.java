package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
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

    private final String dcId;

    private List<ClusterMetaComparator> configChangedClusters = new ArrayList<>();
    private List<ClusterMetaComparatorVisitor> shardOrRedisChangedClusters = new ArrayList<>();

    public DefaultDcMetaChangeManager(String dcId, HealthCheckInstanceManager instanceManager, HealthCheckEndpointFactory healthCheckEndpointFactory) {
        this.dcId = dcId;
        this.instanceManager = instanceManager;
        this.healthCheckEndpointFactory = healthCheckEndpointFactory;
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

        this.current = future;
    }

    private void removeAndAdd() {
        this.shardOrRedisChangedClusters.forEach(clusterMetaComparatorVisitor -> {
            clusterMetaComparatorVisitor.getRedisToDelete().forEach(this::removeRedis);
            clusterMetaComparatorVisitor.getShardsToDelete().forEach(this::removeShard);
        });
        this.configChangedClusters.forEach(clusterMetaComparator -> removeCluster(clusterMetaComparator.getCurrent()));

        this.configChangedClusters.forEach(clusterMetaComparator -> addCluster(clusterMetaComparator.getFuture()));
        this.shardOrRedisChangedClusters.forEach(clusterMetaComparatorVisitor -> {
            clusterMetaComparatorVisitor.getShardsToAdd().forEach(this::addShard);
            clusterMetaComparatorVisitor.getRedisToAdd().forEach(this::addRedis);
        });

        configChangedClusters.clear();
        shardOrRedisChangedClusters.clear();
    }

    private void removeCluster(ClusterMeta removed) {
        if (dcId.equalsIgnoreCase(currentDcId)) {
            logger.info("[removeCluster][{}][{}] remove dc current dc, remove cluster health check", dcId, removed.getId());
            instanceManager.remove(removed.getId());
        }

        logger.info("[removeCluster][{}][{}] remove health check", dcId, removed.getId());
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(removeConsumer)));
        clusterMetaVisitor.accept(removed);
    }

    private void addCluster(ClusterMeta added) {
        if (!isInterestedInCluster(added)) {
            logger.info("[addCluster][{}][skip] cluster not interested", added.getId());
            return;
        }

        logger.info("[addCluster][{}][{}] add health check", dcId, added.getId());
        instanceManager.getOrCreate(added);
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
        clusterMetaVisitor.accept(added);
    }

    private void removeShard(ShardMeta removed) {
        logger.info("[removeShard][{}][{}][{}] remove health check", dcId, removed.parent().getId(), removed.getId());
        ShardMetaVisitor shardMetaVisitor = new ShardMetaVisitor(new RedisMetaVisitor(removeConsumer));
        shardMetaVisitor.accept(removed);
    }

    private void addShard(ShardMeta added) {
        if (!isInterestedInCluster(added.parent())) {
            logger.info("[addShard][{}][{}][{}] cluster not interested", dcId, added.parent().getId(), added.getId());
            return;
        }

        logger.info("[addShard][{}][{}][{}] add health check", dcId, added.parent().getId(), added.getId());
        ShardMetaVisitor shardMetaVisitor = new ShardMetaVisitor(new RedisMetaVisitor(addConsumer));
        shardMetaVisitor.accept(added);
    }

    private void removeRedis(RedisMeta removed) {
        if (null != instanceManager.remove(new HostPort(removed.getIp(), removed.getPort()))) {
            logger.info("[removeRedis][{}:{}] {}", removed.getIp(), removed.getPort(), removed);
        }
    }

    private void addRedis(RedisMeta added) {
        if (!isInterestedInCluster(added.parent().parent())) {
            return;
        }
        logger.info("[addRedis][{}:{}] {}", added.getIp(), added.getPort(), added);
        instanceManager.getOrCreate(added);
    }

    @Override
    public void visitAdded(ClusterMeta added) {
        logger.debug("[visitAdded][{}][{}]", dcId, added.getId());
        addCluster(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) comparator;
        if (comparator.isConfigChange()) {
            this.configChangedClusters.add(clusterMetaComparator);
        } else {
            ClusterMetaComparatorVisitor clusterMetaComparatorVisitor = new ClusterMetaComparatorVisitor();
            clusterMetaComparator.accept(clusterMetaComparatorVisitor);
            this.shardOrRedisChangedClusters.add(clusterMetaComparatorVisitor);
        }
    }


    @Override
    public void visitRemoved(ClusterMeta removed) {
        logger.debug("[visitRemoved][{}][{}]", dcId, removed.getId());
        removeCluster(removed);
    }

    private boolean isInterestedInCluster(ClusterMeta cluster) {
        ClusterType clusterType = ClusterType.lookup(cluster.getType());

        if (clusterType.supportSingleActiveDC() || clusterType.isCrossDc()) {
            return cluster.getActiveDc().equalsIgnoreCase(currentDcId);
        }
        if (clusterType.supportMultiActiveDC()) {
            if (StringUtil.isEmpty(cluster.getDcs())) return false;
            String[] dcs = cluster.getDcs().toLowerCase().split("\\s*,\\s*");
            return Arrays.asList(dcs).contains(currentDcId.toLowerCase());
        }

        return true;
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
}
