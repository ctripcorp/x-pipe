package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface HealthCheckActionFactory<T extends HealthCheckAction, V extends HealthCheckInstance> {

    T create(V instance);

    default boolean supportInstnace(V instance){
        return true;
    }

    default void destroy(T t) throws Exception {
        LifecycleHelper.stopIfPossible(t);
    }
}
