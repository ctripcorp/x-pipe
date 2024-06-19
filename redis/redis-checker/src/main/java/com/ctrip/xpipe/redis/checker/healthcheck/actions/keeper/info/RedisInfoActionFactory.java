package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeper.info;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisWithAssignedLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RedisInfoActionFactory extends AbstractRedisWithAssignedLeaderAwareHealthCheckActionFactory
        implements KeeperSupport, OneWaySupport {

    @Autowired
    private List<RedisInfoActionListener> listeners;

    @Autowired
    private CheckerDbConfig checkerDbConfig;

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    public RedisInfoAction create(RedisHealthCheckInstance instance) {
        RedisInfoAction action = new RedisInfoAction(scheduled, instance, executors, checkerDbConfig);
        action.addListeners(listeners);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return RedisInfoAction.class;
    }
}
