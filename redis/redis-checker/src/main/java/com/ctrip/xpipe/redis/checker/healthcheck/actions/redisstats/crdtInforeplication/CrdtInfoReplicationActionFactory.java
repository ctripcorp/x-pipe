package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication;

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
public class CrdtInfoReplicationActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory implements BiDirectionSupport {

    @Autowired
    private List<CrdtInfoReplicationListener> listeners;

    @Autowired
    private RedisStatsCheckController checkController;
    
    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.CRDT_BACKSTREAMING);
    }

    @Override
    public CrdtInfoReplicationAction create(RedisHealthCheckInstance instance) {
        CrdtInfoReplicationAction action = new CrdtInfoReplicationAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addController(checkController);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
         return CrdtInfoReplicationAction.class;
    }
}
