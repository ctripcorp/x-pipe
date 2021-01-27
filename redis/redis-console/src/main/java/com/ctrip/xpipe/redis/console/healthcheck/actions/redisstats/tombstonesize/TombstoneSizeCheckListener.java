package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

public interface TombstoneSizeCheckListener extends HealthCheckActionListener<TombstoneSizeActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof TombstoneSizeActionContext;
    }

}
