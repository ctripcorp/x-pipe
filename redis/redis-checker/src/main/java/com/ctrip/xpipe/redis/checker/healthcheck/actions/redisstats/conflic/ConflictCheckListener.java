package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface ConflictCheckListener extends HealthCheckActionListener<CrdtConflictCheckContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof CrdtConflictCheckContext;
    }

}
