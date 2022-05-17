package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractCrdtInfoCommandActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
public class CrdtInfoStatsActionFactory extends AbstractCrdtInfoCommandActionFactory<CrdtInfoStatsListener, CrdtInfoStatsAction> implements BiDirectionSupport  {
    
    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    protected CrdtInfoStatsAction createAction(RedisHealthCheckInstance instance) {
        return new CrdtInfoStatsAction(scheduled, instance, executors);
    }
    
    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return CrdtInfoStatsAction.class;
    }
}
