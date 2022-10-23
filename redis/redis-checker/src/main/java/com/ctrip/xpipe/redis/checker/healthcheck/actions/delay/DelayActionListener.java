package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

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

    boolean supportInstance(RedisHealthCheckInstance instance);

}
