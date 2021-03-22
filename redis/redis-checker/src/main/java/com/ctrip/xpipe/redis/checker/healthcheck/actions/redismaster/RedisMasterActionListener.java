package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface RedisMasterActionListener extends HealthCheckActionListener<RedisMasterActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof RedisMasterActionContext;
    }

    @Override
    default void stopWatch(HealthCheckAction action) {
        // do nothing
    }

}
