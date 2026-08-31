package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public interface InfoReplIdActionListener extends HealthCheckActionListener<InfoReplIdActionContext, HealthCheckAction<RedisHealthCheckInstance>> {

    @Override
    default boolean worksfor(ActionContext t) {
        return t instanceof InfoReplIdActionContext;
    }

}
