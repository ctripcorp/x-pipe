package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
@Component
public class DelayActionFactory implements HealthCheckActionFactory<DelayAction>, OneWaySupport, BiDirectionSupport {

    @Autowired
    private PingService pingService;

    @Resource(name = ConsoleContextConfig.PING_DELAY_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.PING_DELAY_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<DelayActionListener> listeners;

    @Autowired
    private List<DelayActionController> controllers;

    @Autowired
    private List<DelayPingActionCollector> delayPingCollectors;

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
        ClusterType clusterType = instance.getRedisInstanceInfo().getClusterType();
        if (clusterType.supportMultiActiveDC()) {
            delayAction = new MultiDcDelayAction(scheduled, instance, executors, pingService);
        } else {
            delayAction = new DelayAction(scheduled, instance, executors, pingService);
        }

        delayAction.addListeners(listenersByClusterType.get(clusterType));
        delayAction.addControllers(controllersByClusterType.get(clusterType));
        if(instance instanceof DefaultRedisHealthCheckInstance) {
            delayAction.addListener(((DefaultRedisHealthCheckInstance)instance).createDelayListener());
        }

        List<DelayPingActionCollector> delayPingActionCollectors = delayPingCollectorByClusterType.get(clusterType);
        delayPingActionCollectors.forEach(collector -> {
            if (collector.supportInstance(instance)) delayAction.addListener(collector.createDelayActionListener());
        });

        return delayAction;
    }
}
