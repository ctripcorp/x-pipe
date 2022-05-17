package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractInfoCommandActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InfoReplicationActionFactory extends AbstractInfoCommandActionFactory<InfoReplicationListener, InfoReplicationAction> implements OneWaySupport {

    @Override
    protected InfoReplicationAction createAction(RedisHealthCheckInstance instance) {
        return new InfoReplicationAction(scheduled, instance, executors);
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
        return InfoReplicationAction.class;
    }
}
