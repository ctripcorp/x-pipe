package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
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
public class DelayActionFactory implements RedisHealthCheckActionFactory<DelayAction>, OneWaySupport, BiDirectionSupport {

    @Autowired
    private PingService pingService;

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<DelayActionListener> listeners;

    @Autowired
    private List<DelayActionController> controllers;

    @Autowired
    private List<DelayPingActionCollector> delayPingCollectors;
    
    @Autowired
    private FoundationService foundationService;

    private Map<ClusterType, List<DelayActionListener>> listenersByClusterType;

    private Map<ClusterType, List<DelayActionController>> controllersByClusterType;

    private Map<ClusterType, List<DelayPingActionCollector>> delayPingCollectorByClusterType;

    @PostConstruct
    public void initDelayActionFactory() {
        listenersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(listeners);
        controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
        delayPingCollectorByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(delayPingCollectors);
    }

    @Override
    public DelayAction create(RedisHealthCheckInstance instance) {
        DelayAction delayAction;
        ClusterType clusterType = instance.getCheckInfo().getClusterType();
        if (clusterType.supportMultiActiveDC()) {
            delayAction = new CRDTDelayAction(scheduled, instance, executors, pingService, foundationService);
        } else if (activeDcCheckerSubscribeMasterTypeInstance(instance)) {
            delayAction = new UpstreamDelayAction(scheduled, instance, executors, pingService, foundationService);
        } else {
            delayAction = new DelayAction(scheduled, instance, executors, pingService, foundationService);
        }

        listenersByClusterType.get(clusterType).forEach(listener -> {
            if (listener.supportInstance(instance)) delayAction.addListener(listener);
        });

        delayAction.addControllers(controllersByClusterType.get(clusterType));
        if (instance instanceof DefaultRedisHealthCheckInstance) {
            delayAction.addListener(((DefaultRedisHealthCheckInstance) instance).createDelayListener());
        }

        List<DelayPingActionCollector> delayPingActionCollectors = delayPingCollectorByClusterType.get(clusterType);
        delayPingActionCollectors.forEach(collector -> {
            if (collector.supportInstance(instance)) delayAction.addListener(collector.createDelayActionListener());
        });

        return delayAction;
    }


    private boolean activeDcCheckerSubscribeMasterTypeInstance(RedisHealthCheckInstance instance) {
        return foundationService.getDataCenter().equalsIgnoreCase(instance.getCheckInfo().getActiveDc()) && !DcGroupType.isNullOrDrMaster(instance.getCheckInfo().getDcGroupType());
    }

}
