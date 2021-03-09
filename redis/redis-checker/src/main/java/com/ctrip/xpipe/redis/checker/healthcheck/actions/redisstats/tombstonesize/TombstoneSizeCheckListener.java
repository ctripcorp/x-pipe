package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface TombstoneSizeCheckListener extends HealthCheckActionListener<TombstoneSizeActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof TombstoneSizeActionContext;
    }

}
