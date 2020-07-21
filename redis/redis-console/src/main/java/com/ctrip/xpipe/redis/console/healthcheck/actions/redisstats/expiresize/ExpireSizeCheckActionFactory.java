package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.expiresize;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.RedisStatsCheckController;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ExpireSizeCheckActionFactory  extends AbstractLeaderAwareHealthCheckActionFactory implements BiDirectionSupport {

    @Autowired
    private List<ExpireSizeCheckListener> listeners;

    @Autowired
    private RedisStatsCheckController checkController;

    @Override
    public ExpireSizeCheckAction create(RedisHealthCheckInstance instance) {
        ExpireSizeCheckAction action = new ExpireSizeCheckAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addController(checkController);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return ExpireSizeCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

}
