package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tombstonesize;

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
public class TombstoneSizeCheckActionFactory  extends AbstractLeaderAwareHealthCheckActionFactory implements BiDirectionSupport {

    @Autowired
    private List<TombstoneSizeMetricListener> listeners;

    @Autowired
    private RedisStatsCheckController checkController;

    @Override
    public TombstoneSizeCheckAction create(RedisHealthCheckInstance instance) {
        TombstoneSizeCheckAction action = new TombstoneSizeCheckAction(scheduled, instance, executors);
        action.addListeners(listeners);
        action.addController(checkController);
        return action;
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return TombstoneSizeCheckAction.class;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

}
