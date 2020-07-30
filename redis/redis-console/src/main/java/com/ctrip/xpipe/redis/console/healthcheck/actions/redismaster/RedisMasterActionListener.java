package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

public interface RedisMasterActionListener extends HealthCheckActionListener<RedisMasterActionContext> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof RedisMasterActionContext;
    }

    @Override
    default void stopWatch(HealthCheckAction action) {
        // do nothing
    }

}
