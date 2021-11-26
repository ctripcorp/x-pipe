package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationListener;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractRedisLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

public abstract class AbstractInfoCommandActionFactory<T extends AbstractInfoListener, C extends AbstractInfoCommandAction> extends AbstractRedisLeaderAwareHealthCheckActionFactory {
    @Autowired
    private List<T> listeners;

    @Autowired
    private RedisStatsCheckController checkController;

    @Override
    public C create(RedisHealthCheckInstance instance) {
        C action = createAction(instance);
        action.addListeners(listeners);
        action.addController(checkController);
        return action;
    }
    
    protected abstract C createAction(RedisHealthCheckInstance instance);
    
}
