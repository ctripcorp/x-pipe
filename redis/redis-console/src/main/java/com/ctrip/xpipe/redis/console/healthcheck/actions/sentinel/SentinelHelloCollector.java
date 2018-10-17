package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public interface SentinelHelloCollector extends HealthCheckActionListener<SentinelActionContext> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof SentinelActionContext;
    }
}
