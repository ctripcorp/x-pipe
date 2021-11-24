package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface CrdtInfoReplicationListener extends HealthCheckActionListener<CrdtInfoReplicationContext, HealthCheckAction> {
    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof CrdtInfoReplicationContext;
    }
    
}
