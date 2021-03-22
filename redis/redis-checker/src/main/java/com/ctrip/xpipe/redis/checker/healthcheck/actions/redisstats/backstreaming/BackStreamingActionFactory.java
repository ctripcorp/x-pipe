package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.backstreaming;

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

/**
 * @author lishanglin
 * date 2021/1/26
 */
@Component
public class BackStreamingActionFactory extends AbstractRedisLeaderAwareHealthCheckActionFactory implements BiDirectionSupport {

    @Autowired
    private List<BackStreamingListener> listeners;

    @Autowired
    private RedisStatsCheckController checkController;

    @Override
    public BackStreamingAction create(RedisHealthCheckInstance instance) {
        BackStreamingAction action = new BackStreamingAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addController(checkController);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return BackStreamingAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.CRDT_BACKSTREAMING);
    }

}
