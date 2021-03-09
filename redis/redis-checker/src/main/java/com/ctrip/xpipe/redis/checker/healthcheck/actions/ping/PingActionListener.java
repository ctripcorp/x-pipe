package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface PingActionListener extends HealthCheckActionListener<PingActionContext, HealthCheckAction<RedisHealthCheckInstance>> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof PingActionContext;
    }
}
