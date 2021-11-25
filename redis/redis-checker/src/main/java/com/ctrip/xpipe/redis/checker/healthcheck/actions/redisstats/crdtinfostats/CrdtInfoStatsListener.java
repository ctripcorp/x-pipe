package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface CrdtInfoStatsListener extends HealthCheckActionListener<CrdtInfoStatsContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof CrdtInfoStatsContext;
    }

}
