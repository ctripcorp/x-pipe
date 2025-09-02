package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.psubscribe.PsubPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
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

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
@Component
public class PingActionFactory implements RedisHealthCheckActionFactory<PingAction>, OneWaySupport, BiDirectionSupport {

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private List<PingActionListener> listeners;

    @Autowired
    private List<PingActionController> controllers;

    @Autowired
    private List<DelayPingActionCollector> delayPingCollectors;

    @Autowired
    private List<PsubPingActionCollector> psubPingActionCollectors;

    private Map<ClusterType, List<PingActionController>> controllersByClusterType;

    private Map<ClusterType, List<PingActionListener>> listenerByClusterType;

    private Map<ClusterType, List<DelayPingActionCollector>> delayPingCollectorsByClusterType;

    private Map<ClusterType, List<PsubPingActionCollector>> psubPingActionCollectorsByClusterType;

    protected static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @PostConstruct
    public void postConstruct() {
        controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
        listenerByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(listeners);
        delayPingCollectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(delayPingCollectors);
        psubPingActionCollectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(psubPingActionCollectors);
    }

    @Override
    public PingAction create(RedisHealthCheckInstance instance) {
        PingAction pingAction = new PingAction(scheduled, instance, executors);
        ClusterType clusterType = instance.getCheckInfo().getClusterType();

        pingAction.addControllers(controllersByClusterType.get(clusterType));
        String activeDc = instance.getCheckInfo().getActiveDc();
        if (currentDcId.equalsIgnoreCase(activeDc) || ClusterType.BI_DIRECTION.equals(instance.getCheckInfo().getClusterType())) {
            pingAction.addListeners(listenerByClusterType.get(clusterType));
            if(instance instanceof DefaultRedisHealthCheckInstance) {
                pingAction.addListener(((DefaultRedisHealthCheckInstance)instance).createPingListener());
            }
            delayPingCollectorsByClusterType.get(clusterType).forEach(collector -> {
                if (collector.supportInstance(instance)) {
                    pingAction.addListener(collector.createPingActionListener());
                    collector.createHealthStatus(instance);
                }
            });
        } else if (activeDc != null && metaCache.isCrossRegion(currentDcId, activeDc)) {
            psubPingActionCollectorsByClusterType.get(clusterType).forEach(collector -> {
                if (collector.supportInstance(instance)) {
                    pingAction.addListener(collector.createPingActionListener());
                    collector.createHealthStatus(instance);
                }
            });
        }

        return pingAction;
    }
}
