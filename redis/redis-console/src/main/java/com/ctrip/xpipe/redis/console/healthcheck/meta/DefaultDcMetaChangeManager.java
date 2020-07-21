package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
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

    public DefaultDcMetaChangeManager(HealthCheckInstanceManager instanceManager) {
        this.instanceManager = instanceManager;
    }

    @Override
    public void compare(DcMeta future) {
        // init
        if(current == null) {
            current = future;
            return;
        }

        // normal logic
        DcMetaComparator comparator = DcMetaComparator.buildComparator(current, future);
        comparator.accept(this);
        current = future;
    }


    @Override
    public void visitAdded(ClusterMeta added) {
        if (!isInterestedInCluster(added)) {
            return;
        }
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
        clusterMetaVisitor.accept(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) comparator;
        updateDcInterested(clusterMetaComparator);
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
            for (ShardMeta shardMeta : comparator.getFuture().getShards().values()) {
                for (RedisMeta redisMeta : shardMeta.getRedises()) {
                    instanceManager.getOrCreate(redisMeta);
                }
            }
        } else {
            logger.info("[updateDcInterested] loss interested {}", comparator.getFuture());
            for (ShardMeta shardMeta : comparator.getCurrent().getShards().values()) {
                for (RedisMeta redisMeta : shardMeta.getRedises()) {
                    instanceManager.remove(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                }
            }
        }
    }

    @Override
    public void visitRemoved(ClusterMeta removed) {
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
            instanceManager.getOrCreate(redisMeta).getRedisInstanceInfo().isMaster(redisMeta.isMaster());
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
