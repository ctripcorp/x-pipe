package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tps;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

public interface TpsCheckListener extends HealthCheckActionListener<TpsActionContext> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof TpsActionContext;
    }

}
