package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.MetaComparatorVisitor;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.DcMetaComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultDcMetaChangeManager implements DcMetaChangeManager, MetaComparatorVisitor<ClusterMeta> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDcMetaChangeManager.class);

    private DcMeta current;

    private HealthCheckInstanceManager instanceManager;

    private AtomicBoolean started = new AtomicBoolean(true);

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
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
        clusterMetaVisitor.accept(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ((ClusterMetaComparator)comparator).accept(new ClusterMetaComparatorVisitor(addConsumer, removeConsumer, redisChanged));
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
            logger.info("[Redis-Add] {}", redisMeta);
            RedisHealthCheckInstance instance = instanceManager.getOrCreate(redisMeta);
            try {
                LifecycleHelper.startIfPossible(instance);
            } catch (Exception e) {
                logger.error("[clusterAdded]", e);
            }
        }
    };

    private Consumer<RedisMeta> redisChanged = new Consumer<RedisMeta>() {
        @Override
        public void accept(RedisMeta redisMeta) {
            logger.info("[Redis-Change] {}, master: {}", redisMeta, redisMeta.isMaster());
            instanceManager.getOrCreate(redisMeta).getRedisInstanceInfo().isMaster(redisMeta.isMaster());
        }
    };

    @Override
    public void start() {
        if(started.compareAndSet(false, true)) {

            if (current == null) {
                logger.error("[start] cannot start without a DcMeta");
            }
            for (ClusterMeta cluster : current.getClusters().values()) {
                visitAdded(cluster);
            }
        }
    }

    @Override
    public void stop() {
        if(started.compareAndSet(true, false)) {
            for (ClusterMeta cluster : current.getClusters().values()) {
                visitRemoved(cluster);
            }
            current = null;
        }
    }
}
