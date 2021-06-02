package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 3:42 PM
 */
public interface InfoActionListener extends HealthCheckActionListener<RawInfoActionContext, HealthCheckAction<RedisHealthCheckInstance>> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof RawInfoActionContext;
    }
}
