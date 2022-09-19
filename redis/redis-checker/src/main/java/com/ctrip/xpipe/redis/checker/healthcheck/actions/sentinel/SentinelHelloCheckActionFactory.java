package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractClusterLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.HELLO_CHECK_EXECUTORS;
import static com.ctrip.xpipe.redis.checker.resource.Resource.HELLO_CHECK_SCHEDULED;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class SentinelHelloCheckActionFactory extends AbstractClusterLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport, SingleDcSupport, LocalDcSupport, CrossDcSupport, HeteroSupport {

    private Map<Pair<ClusterType, DcGroupType>, List<SentinelHelloCollector>> collectorsByClusterType;

    private Map<Pair<ClusterType, DcGroupType>, List<SentinelActionController>> controllersByClusterType;

    private CheckerDbConfig checkerDbConfig;

    private PersistenceCache persistenceCache;

    private MetaCache metaCache;

    @Resource(name = HELLO_CHECK_SCHEDULED)
    private ScheduledExecutorService helloCheckScheduled;

    @Resource(name = HELLO_CHECK_EXECUTORS)
    private ExecutorService helloCheckExecutors;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    public SentinelHelloCheckActionFactory(List<SentinelHelloCollector> collectors, List<SentinelActionController> controllers,
                                           CheckerConfig checkerConfig, CheckerDbConfig checkerDbConfig, PersistenceCache persistenceCache, MetaCache metaCache) {
        this.checkerDbConfig = checkerDbConfig;
        this.persistenceCache = persistenceCache;
        this.collectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterTypeAndGroupType(collectors);
        this.controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterTypeAndGroupType(controllers);
        this.metaCache = metaCache;
    }

    @Override
    public SiteLeaderAwareHealthCheckAction create(ClusterHealthCheckInstance instance) {
        SentinelHelloCheckAction action = new SentinelHelloCheckAction(helloCheckScheduled, instance, helloCheckExecutors, checkerDbConfig, persistenceCache, metaCache, healthCheckInstanceManager);
        ClusterType clusterType = instance.getCheckInfo().getClusterType();
        action.addListeners(clusterType.equals(ClusterType.HETERO) ? getListenersForHetero(instance) : collectorsByClusterType.get(new Pair<>(clusterType, DcGroupType.DR_MASTER)));
        action.addControllers(clusterType.equals(ClusterType.HETERO) ? getControllersForHetero(instance) : controllersByClusterType.get(new Pair<>(clusterType, DcGroupType.DR_MASTER)));
        return action;
    }

    List<SentinelHelloCollector> getListenersForHetero(ClusterHealthCheckInstance instance) {
        return !instance.getCheckInfo().getDcGroupType().isValue() && metaCache.isCrossRegion(instance.getCheckInfo().getActiveDc(), currentDcId) ?
                collectorsByClusterType.get(new Pair<>(ClusterType.HETERO, DcGroupType.MASTER)) : collectorsByClusterType.get(new Pair<>(ClusterType.HETERO, DcGroupType.DR_MASTER));
    }

    List<SentinelActionController> getControllersForHetero(ClusterHealthCheckInstance instance) {
        return !instance.getCheckInfo().getDcGroupType().isValue() && metaCache.isCrossRegion(instance.getCheckInfo().getActiveDc(), currentDcId) ?
                controllersByClusterType.get(new Pair<>(ClusterType.HETERO, DcGroupType.MASTER)) : controllersByClusterType.get(new Pair<>(ClusterType.HETERO, DcGroupType.DR_MASTER));
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return SentinelHelloCheckAction.class;
    }

    @VisibleForTesting
    protected SentinelHelloCheckActionFactory setCheckerDbConfig(CheckerDbConfig checkerDbConfig) {
        this.checkerDbConfig = checkerDbConfig;
        return this;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS);
    }
}
