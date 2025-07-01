package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;

public interface AbstractInfoListener<T extends AbstractInfoContext>  extends HealthCheckActionListener<T, HealthCheckAction> {
    @Override 
    default boolean worksfor(ActionContext t) {
        try {
            T temp = (T) t; 
            return true;
        } catch (ClassCastException e) {
            return false;
        }
    }

}
