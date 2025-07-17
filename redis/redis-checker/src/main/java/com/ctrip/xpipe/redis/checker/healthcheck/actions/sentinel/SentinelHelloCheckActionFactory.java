package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractClusterLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.resource.Resource.HELLO_CHECK_EXECUTORS;
import static com.ctrip.xpipe.redis.checker.resource.Resource.HELLO_CHECK_SCHEDULED;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class SentinelHelloCheckActionFactory extends AbstractClusterLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport, SingleDcSupport, LocalDcSupport, CrossDcSupport, CrossRegionSupport {

    private Map<ClusterType, List<SentinelHelloCollector>> collectorsByClusterType;

    private Map<ClusterType, List<SentinelActionController>> controllersByClusterType;

    private List<SentinelHelloCollector> collectors4CrossRegion;

    private List<SentinelActionController> controllers4CrossRegion;

    private CheckerDbConfig checkerDbConfig;

    private PersistenceCache persistenceCache;

    private MetaCache metaCache;

    @Resource(name = HELLO_CHECK_SCHEDULED)
    private ScheduledExecutorService helloCheckScheduled;

    @Resource(name = HELLO_CHECK_EXECUTORS)
    private ExecutorService helloCheckExecutors;

    private final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    public SentinelHelloCheckActionFactory(List<SentinelHelloCollector> collectors, List<SentinelActionController> controllers,
                                           CheckerConfig checkerConfig, CheckerDbConfig checkerDbConfig, PersistenceCache persistenceCache, MetaCache metaCache) {
        this.checkerDbConfig = checkerDbConfig;
        this.persistenceCache = persistenceCache;
        this.collectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(collectors);
        this.controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
        this.metaCache = metaCache;
        this.collectors4CrossRegion = collectors.stream().filter(collector -> collector instanceof CrossRegionSupport).collect(Collectors.toList()) ;
        this.controllers4CrossRegion = controllers.stream().filter(controller -> controller instanceof CrossRegionSupport).collect(Collectors.toList()) ;
    }

    @Override
    public SiteLeaderAwareHealthCheckAction create(ClusterHealthCheckInstance instance) {
        SentinelHelloCheckAction action = new SentinelHelloCheckAction(helloCheckScheduled, instance, helloCheckExecutors, checkerDbConfig, persistenceCache, metaCache, healthCheckInstanceManager);

        ClusterInstanceInfo info = instance.getCheckInfo();
        ClusterType clusterType = info.getClusterType();
        String azGroupType = info.getAzGroupType();
        ClusterType azGroupClusterType = StringUtil.isEmpty(azGroupType) ? null : ClusterType.lookup(azGroupType);
        if (clusterType == ClusterType.ONE_WAY && azGroupClusterType == ClusterType.SINGLE_DC) {
            action.addListeners(collectorsByClusterType.get(azGroupClusterType));
            action.addControllers(controllersByClusterType.get(azGroupClusterType));
        } else if (clusterType == ClusterType.ONE_WAY && isBackupDcAndCrossRegion(currentDc, info.getActiveDc(), info.getDcs())) {
            action.addListeners(collectors4CrossRegion);
            action.addControllers(controllers4CrossRegion);
        }else {
            action.addListeners(collectorsByClusterType.get(clusterType));
            action.addControllers(controllersByClusterType.get(clusterType));
        }

        return action;
    }

    protected boolean isBackupDcAndCrossRegion(String currentDc, String activeDc, List<String> dcs) {
        return metaCache.isCrossRegion(activeDc, currentDc) && dcs.contains(currentDc);
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
