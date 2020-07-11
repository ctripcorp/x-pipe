package com.ctrip.xpipe.redis.console.healthcheck.actions.ping;


import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import org.springframework.beans.factory.annotation.Autowired;
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
public class PingActionFactory implements HealthCheckActionFactory<PingAction>, OneWaySupport, BiDirectionSupport {

    @Resource(name = ConsoleContextConfig.PING_DELAY_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = ConsoleContextConfig.PING_DELAY_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<PingActionListener> listeners;

    @Autowired
    private List<PingActionController> controllers;

    @Autowired
    private List<DelayPingActionCollector> delayPingCollectors;

    private Map<ClusterType, List<PingActionController>> controllersByClusterType;

    private Map<ClusterType, List<PingActionListener>> listenerByClusterType;

    private Map<ClusterType, List<DelayPingActionCollector>> delayPingCollectorsByClusterType;

    @PostConstruct
    public void postConstruct() {
        controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
        listenerByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(listeners);
        delayPingCollectorsByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(delayPingCollectors);
    }

    @Override
    public PingAction create(RedisHealthCheckInstance instance) {
        PingAction pingAction = new PingAction(scheduled, instance, executors);
        ClusterType clusterType = instance.getRedisInstanceInfo().getClusterType();

        pingAction.addListeners(listenerByClusterType.get(clusterType));
        pingAction.addControllers(controllersByClusterType.get(clusterType));
        if(instance instanceof DefaultRedisHealthCheckInstance) {
            pingAction.addListener(((DefaultRedisHealthCheckInstance)instance).createPingListener());
        }

        delayPingCollectorsByClusterType.get(clusterType).forEach(collector -> {
            if (collector.supportInstance(instance)) pingAction.addListener(collector.createPingActionListener());
        });

        return pingAction;
    }
}
