package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.util.ClusterTypeSupporterSeparator;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component
public class RedisMasterCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory implements OneWaySupport, BiDirectionSupport {

    private Map<ClusterType, List<RedisMasterController>> controllersByClusterType;

    private Map<ClusterType, List<RedisMasterActionListener>> listenersByClusterType;

    @Autowired
    public RedisMasterCheckActionFactory(List<RedisMasterController> controllers, List<RedisMasterActionListener> listeners) {
        this.controllersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(controllers);
        this.listenersByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(listeners);
    }

    @Override
    public SiteLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance) {
        RedisMasterCheckAction action = new RedisMasterCheckAction(scheduled, instance, executors);
        ClusterType clusterType = instance.getRedisInstanceInfo().getClusterType();
        action.addControllers(controllersByClusterType.get(clusterType));
        action.addListeners(listenersByClusterType.get(clusterType));

        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return RedisMasterCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }
}
