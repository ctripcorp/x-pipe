package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

public interface ConflictCheckListener extends HealthCheckActionListener<CrdtConflictCheckContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof CrdtConflictCheckContext;
    }

}
