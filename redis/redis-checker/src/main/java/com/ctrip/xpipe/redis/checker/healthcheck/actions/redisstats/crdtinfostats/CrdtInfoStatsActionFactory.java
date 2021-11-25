package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.RedisStatsCheckController;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
public class CrdtInfoStatsActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory implements BiDirectionSupport {
    @Autowired
    private List<CrdtInfoStatsListener> listeners;

    @Autowired
    private RedisStatsCheckController checkController;

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    public CrdtInfoStatsAction create(RedisHealthCheckInstance instance) {
        CrdtInfoStatsAction action = new CrdtInfoStatsAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addController(checkController);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return CrdtInfoStatsAction.class;
    }
}
