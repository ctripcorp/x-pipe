package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface HealthCheckActionFactory<T extends HealthCheckAction> {

    T create(RedisHealthCheckInstance instance);

    default void destroy(T t) throws Exception {
        LifecycleHelper.stopIfPossible(t);
    }
}
