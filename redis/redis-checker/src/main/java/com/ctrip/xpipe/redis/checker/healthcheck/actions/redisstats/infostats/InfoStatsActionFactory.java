package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoCommandActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InfoStatsActionFactory extends AbstractInfoCommandActionFactory<InfoStatsListener, InfoStatsAction> implements OneWaySupport {

    @Override
    protected InfoStatsAction createAction(RedisHealthCheckInstance instance) {
        return new InfoStatsAction(scheduled, instance, executors);
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return InfoStatsAction.class;
    }
}
