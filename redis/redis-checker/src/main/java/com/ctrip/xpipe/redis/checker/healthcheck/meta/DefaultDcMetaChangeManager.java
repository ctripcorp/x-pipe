package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcRouteMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.RouteMetaComparator;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
        DcRouteMetaComparator dcRouteMetaComparator = new DcRouteMetaComparator(current, future);
        //change routes
        if(dcRouteMetaComparator.getAdded().size() != 0
                || dcRouteMetaComparator.getMofified().size() != 0
                || dcRouteMetaComparator.getRemoved().size() != 0) {
            healthCheckEndpointFactory.updateRoutes();
        }
        comparator.accept(this);
        
        current = future;
        
    }
    

    @Override
    public void visitAdded(ClusterMeta added) {
        if (!isInterestedInCluster(added)) {
            return;
        }

        logger.info("[visitAdded][{}][{}] add cluster health check", dcId, added.getId());
        instanceManager.getOrCreate(added);
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
        clusterMetaVisitor.accept(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) comparator;
        updateDcInterested(clusterMetaComparator);
        updateClusterMeta(clusterMetaComparator.getFuture());
        clusterMetaComparator.accept(new ClusterMetaComparatorVisitor(addConsumer, removeConsumer, redisChanged));
    }

    private void updateDcInterested(ClusterMetaComparator comparator) {
        boolean currentInterested = isInterestedInCluster(comparator.getCurrent());
        boolean futureInterested = isInterestedInCluster(comparator.getFuture());
        if (currentInterested == futureInterested) {
            logger.debug("[updateDcInterested] interested not change: {}", futureInterested);
            return;
        }

        if (futureInterested) {
            logger.info("[updateDcInterested] become interested {}", comparator.getFuture());
            instanceManager.getOrCreate(comparator.getFuture());
            for (ShardMeta shardMeta : comparator.getFuture().getShards().values()) {
                for (RedisMeta redisMeta : shardMeta.getRedises()) {
                    instanceManager.getOrCreate(redisMeta);
                }
            }
        } else {
            logger.info("[updateDcInterested] loss interested {}", comparator.getFuture());
            instanceManager.remove(comparator.getCurrent().getId());
            for (ShardMeta shardMeta : comparator.getCurrent().getShards().values()) {
                for (RedisMeta redisMeta : shardMeta.getRedises()) {
                    instanceManager.remove(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                }
            }
        }
    }

    private void updateClusterMeta(ClusterMeta future) {
        ClusterHealthCheckInstance checkInstance = instanceManager.findClusterHealthCheckInstance(future.getId());

        if (null != checkInstance) {
            checkInstance.getCheckInfo().setOrgId(future.getOrgId()).setActiveDc(future.getActiveDc());
            for (ShardMeta shardMeta : future.getShards().values()) {
                for (RedisMeta redisMeta : shardMeta.getRedises()) {
                    RedisHealthCheckInstance redisInstance = instanceManager.findRedisHealthCheckInstance(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                    if (redisInstance != null) {
                        redisInstance.getCheckInfo().setActiveDc(future.getActiveDc());
                    }
                }
            }
        }
    }

    @Override
    public void visitRemoved(ClusterMeta removed) {
        if (!isInterestedInCluster(removed)) return;

        logger.debug("[visitRemoved][{}][{}]", dcId, removed.getId());
        if (dcId.equalsIgnoreCase(currentDcId)) {
            logger.info("[visitRemoved][{}][{}] remove dc current dc, remove cluster health check", dcId, removed.getId());
            instanceManager.remove(removed.getId());
        }

        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(removeConsumer)));
        clusterMetaVisitor.accept(removed);
    }

    private boolean isInterestedInCluster(ClusterMeta cluster) {
        ClusterType clusterType = ClusterType.lookup(cluster.getType());
        if (clusterType.supportSingleActiveDC()) {
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
            logger.info("[Redis-Deleted] {}", redisMeta);
            instanceManager.remove(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
        }
    };

    private Consumer<RedisMeta> addConsumer = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            if (!isInterestedInCluster(redisMeta.parent().parent())) {
                return;
            }
            logger.info("[Redis-Add] {}", redisMeta);
            instanceManager.getOrCreate(redisMeta);
        }
    };

    private Consumer<RedisMeta> redisChanged = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            if (!isInterestedInCluster(redisMeta.parent().parent())) {
                return;
            }
            logger.info("[Redis-Change] {}, master: {}", redisMeta, redisMeta.isMaster());
            instanceManager.getOrCreate(redisMeta).getCheckInfo().isMaster(redisMeta.isMaster());
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
