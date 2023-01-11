package com.ctrip.xpipe.redis.checker.healthcheck.actions.gtidgap;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface GtidGapCheckActionListener extends HealthCheckActionListener<GtidGapCheckActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof GtidGapCheckActionContext;
    }

}
