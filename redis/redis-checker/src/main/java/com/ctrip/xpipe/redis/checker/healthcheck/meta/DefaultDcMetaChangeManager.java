package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
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

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * @author chen.zhu
 * <p>
 * Aug 28, 2018
 */
public class DefaultDcMetaChangeManager extends AbstractStartStoppable implements DcMetaChangeManager, MetaComparatorVisitor<ClusterMeta> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDcMetaChangeManager.class);

    private MetaCache metaCache;

    private DcMeta current;

    private HealthCheckInstanceManager instanceManager;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();
    
    private HealthCheckEndpointFactory healthCheckEndpointFactory;

    private CheckerConfig checkerConfig;

    private final String dcId;
    public DefaultDcMetaChangeManager(String dcId, HealthCheckInstanceManager instanceManager, HealthCheckEndpointFactory healthCheckEndpointFactory, MetaCache metaCache, CheckerConfig checkerConfig) {
        this.dcId = dcId;
        this.instanceManager = instanceManager;
        this.healthCheckEndpointFactory = healthCheckEndpointFactory;
        this.metaCache = metaCache;
        this.checkerConfig = checkerConfig;
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
        if(!dcRouteMetaComparator.getAdded().isEmpty()
                || !dcRouteMetaComparator.getMofified().isEmpty()
                || !dcRouteMetaComparator.getRemoved().isEmpty()) {
            healthCheckEndpointFactory.updateRoutes();
        }
        comparator.accept(this);
        comparator.getMofified().stream().map(metaComparator -> ((ClusterMetaComparator) metaComparator).getCurrent())
                .forEach(this::removeCluster);
        comparator.getMofified().stream().map(metaComparator -> ((ClusterMetaComparator) metaComparator).getFuture())
                .forEach(this::addCluster);

        this.current = future;
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
            logger.info("[addCluster][cluster not interested] {}", added.getId());
            return;
        }

        logger.info("[addCluster][{}][{}] add health check", dcId, added.getId());
        instanceManager.getOrCreate(added);
        ClusterMetaVisitor clusterMetaVisitor = new ClusterMetaVisitor(new ShardMetaVisitor(new RedisMetaVisitor(addConsumer)));
        clusterMetaVisitor.accept(added);
    }
    

    @Override
    public void visitAdded(ClusterMeta added) {
        logger.debug("[visitAdded][{}][{}]", dcId, added.getId());
        addCluster(added);
    }

    @Override
    public void visitModified(MetaComparator comparator) {
        ClusterMetaComparator clusterMetaComparator = (ClusterMetaComparator) comparator;
        if (((ClusterMetaComparator) comparator).metaChange()) {
            reloadCluster();
        } else {
            clusterMetaComparator.accept(new ClusterMetaComparatorVisitor(addConsumer, removeConsumer, redisChanged));
        }
    }


    @Override
    public void visitRemoved(ClusterMeta removed) {
        logger.debug("[visitRemoved][{}][{}]", dcId, removed.getId());
        removeCluster(removed);
    }

    private boolean isInterestedInCluster(ClusterMeta cluster) {
        ClusterType clusterType = ClusterType.lookup(cluster.getType());
        if (clusterType.equals(ClusterType.CROSS_DC)) {
            return isMaxMasterCountInCurrentDc(cluster);
        }
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

    boolean isMaxMasterCountInCurrentDc(ClusterMeta clusterMeta) {
        Pair<String, Integer> maxMasterCountDc = metaCache.getMaxMasterCountDc(clusterMeta.getId(), checkerConfig.getIgnoredHealthCheckDc());
        return maxMasterCountDc != null && maxMasterCountDc.getValue() > 0 && maxMasterCountDc.getKey().equalsIgnoreCase(currentDcId);
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
