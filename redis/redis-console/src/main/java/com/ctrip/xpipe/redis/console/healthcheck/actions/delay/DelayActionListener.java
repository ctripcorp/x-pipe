package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface DelayActionListener extends RedisHealthCheckActionListener<DelayActionContext> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof DelayActionContext;
    }
}
