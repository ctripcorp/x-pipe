package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.backstreaming;

import com.ctrip.xpipe.redis.console.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public interface BackStreamingListener extends HealthCheckActionListener<BackStreamingContext, HealthCheckAction> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof BackStreamingContext;
    }

}