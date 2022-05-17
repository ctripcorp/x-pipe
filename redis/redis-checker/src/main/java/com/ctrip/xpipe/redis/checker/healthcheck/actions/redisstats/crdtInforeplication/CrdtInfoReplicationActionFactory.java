package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.AbstractCrdtInfoCommandActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckAction;
import com.google.common.collect.Lists;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CrdtInfoReplicationActionFactory extends AbstractCrdtInfoCommandActionFactory<CrdtInfoReplicationListener, CrdtInfoReplicationAction> implements BiDirectionSupport {
    
    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList(ALERT_TYPE.CRDT_BACKSTREAMING);
    }



    @Override
    public Class<? extends SiteLeaderAwareHealthCheckAction> support() {
         return CrdtInfoReplicationAction.class;
    }

    @Override
    protected CrdtInfoReplicationAction createAction(RedisHealthCheckInstance instance) {
        return new CrdtInfoReplicationAction(scheduled, instance, executors);
    }
}
