package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public interface SentinelHelloCollector extends HealthCheckActionListener<SentinelActionContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return !(t instanceof NoRedisToSubContext) && t instanceof SentinelActionContext;
    }
}
