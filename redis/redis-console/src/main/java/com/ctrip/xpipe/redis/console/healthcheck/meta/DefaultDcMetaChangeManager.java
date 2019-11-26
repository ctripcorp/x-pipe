package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (!added.getActiveDc().equalsIgnoreCase(FoundationService.DEFAULT.getDataCenter())) {
            return;
        }
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
        clusterMetaVisitor.accept(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) comparator;
        updateActiveDc(clusterMetaComparator);
        clusterMetaComparator.accept(new ClusterMetaComparatorVisitor(addConsumer, removeConsumer, redisChanged));
    }

    private void updateActiveDc(ClusterMetaComparator comparator) {
        ClusterMeta current = comparator.getCurrent(), future = comparator.getFuture();
        if (current.getActiveDc().equals(future.getActiveDc())) {
            logger.warn("[updateActiveDc][{}][previous-dc {}][future-dc {}]", current.getId(),
                    current.getActiveDc(), future.getActiveDc());
            return;
        }
        installIfClusterActiveIdcCurrentIdc(comparator);
        uninstallIfClusterActiveIdcWasCurrentIdc(comparator);
    }

    private void installIfClusterActiveIdcCurrentIdc(ClusterMetaComparator comparator) {
        ClusterMeta future = comparator.getFuture();
        if (!FoundationService.DEFAULT.getDataCenter().equals(future.getActiveDc())) {
            return;
        }
        for (ShardMeta shardMeta : future.getShards().values()) {
            for (RedisMeta redisMeta : shardMeta.getRedises()) {
                instanceManager.getOrCreate(redisMeta);
            }
        }
    }

    private void uninstallIfClusterActiveIdcWasCurrentIdc(ClusterMetaComparator comparator) {
        ClusterMeta current = comparator.getCurrent();
        if (!FoundationService.DEFAULT.getDataCenter().equals(current.getActiveDc())) {
            return;
        }
        for (ShardMeta shardMeta : current.getShards().values()) {
            for (RedisMeta redisMeta : shardMeta.getRedises()) {
                instanceManager.remove(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
            }
        }
    }

    @Override
    public void visitRemoved(ClusterMeta removed) {
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(removeConsumer)));
        clusterMetaVisitor.accept(removed);
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
            if (!redisMeta.parent().getActiveDc().equalsIgnoreCase(FoundationService.DEFAULT.getDataCenter())) {
                return;
            }
            logger.info("[Redis-Add] {}", redisMeta);
            instanceManager.getOrCreate(redisMeta);
        }
    };

    private Consumer<RedisMeta> redisChanged = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            if (!redisMeta.parent().getActiveDc().equalsIgnoreCase(FoundationService.DEFAULT.getDataCenter())) {
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
