package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface TpsCheckListener extends HealthCheckActionListener<TpsActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof TpsActionContext;
    }

}
