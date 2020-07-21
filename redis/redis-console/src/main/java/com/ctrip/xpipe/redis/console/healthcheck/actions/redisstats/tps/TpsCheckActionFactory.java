package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TpsCheckActionFactory extends AbstractLeaderAwareHealthCheckActionFactory implements BiDirectionSupport {

    @Autowired
    private List<TpsCheckListener> listeners;

    @Autowired
    private List<TpsCheckController> controllers;

    @Override
    public TpsCheckAction create(RedisHealthCheckInstance instance) {
        TpsCheckAction action = new TpsCheckAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addControllers(controllers);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return TpsCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

}
