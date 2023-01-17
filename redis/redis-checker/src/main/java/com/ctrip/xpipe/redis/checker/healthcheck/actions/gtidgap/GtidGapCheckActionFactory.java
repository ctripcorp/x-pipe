package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
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

@Component
public class GtidGapCheckActionFactory implements RedisHealthCheckActionFactory<GtidGapCheckAction>, OneWaySupport {

    @Resource(name = PING_DELAY_INFO_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = PING_DELAY_INFO_EXECUTORS)
    private ExecutorService executors;

    @Autowired
    private List<GtidGapCheckActionListener> listeners;

    @Autowired
    private List<GtidGapCheckActionController> controllers;

    private Map<ClusterType, List<GtidGapCheckActionListener>> listenersByClusterType;

    private Map<ClusterType, List<GtidGapCheckActionController>> controllersByClusterType;


    @PostConstruct
    public void initDelayActionFactory() {
        listenersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(listeners);
        controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
    }

    @Override
    public GtidGapCheckAction create(RedisHealthCheckInstance instance) {
        GtidGapCheckAction gtidGapCheckAction = new GtidGapCheckAction(scheduled, instance, executors);
        ClusterType clusterType = instance.getCheckInfo().getClusterType();
        gtidGapCheckAction.addControllers(controllersByClusterType.get(clusterType));
        gtidGapCheckAction.addListeners(listenersByClusterType.get(clusterType));
        return gtidGapCheckAction;
    }

}
