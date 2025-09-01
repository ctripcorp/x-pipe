package com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_EXECUTORS;
import static com.ctrip.xpipe.redis.checker.resource.Resource.PING_DELAY_INFO_SCHEDULED;

@Component
public class PsubActionFactory implements RedisHealthCheckActionFactory<PsubAction>, OneWaySupport {

    @Autowired
    private MetaCache metaCache;

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<PsubActionController> controllers;

    @Autowired
    private List<PsubPingActionCollector> psubPingActionCollectors;

    private Map<ClusterType, List<PsubActionController>> controllersByClusterType;

    private Map<ClusterType, List<PsubPingActionCollector>> psubPingActionCollectorsByClusterType;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @PostConstruct
    public void postConstruct() {
        controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
        psubPingActionCollectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(psubPingActionCollectors);
    }

    @Override
    public PsubAction create(RedisHealthCheckInstance instance) {
        PsubAction psubAction = new PsubAction(scheduled, instance, executors);
        ClusterType clusterType = instance.getCheckInfo().getClusterType();

        psubAction.addControllers(controllersByClusterType.get(clusterType));
        psubPingActionCollectorsByClusterType.get(clusterType).forEach(collector -> {
            if (collector.supportInstance(instance)) psubAction.addListener(collector.createPsubActionListener(instance));
        });
        return psubAction;
    }

    @Override
    public boolean supportInstnace(RedisHealthCheckInstance instance) {
        RedisInstanceInfo info = instance.getCheckInfo();
        return metaCache.isCrossRegion(currentDcId, info.getActiveDc()) && currentDcId.equalsIgnoreCase(info.getDcId());
    }
}
