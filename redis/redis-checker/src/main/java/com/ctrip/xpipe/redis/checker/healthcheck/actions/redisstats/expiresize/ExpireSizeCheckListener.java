package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface ExpireSizeCheckListener extends HealthCheckActionListener<ExpireSizeActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof ExpireSizeActionContext;
    }

}
