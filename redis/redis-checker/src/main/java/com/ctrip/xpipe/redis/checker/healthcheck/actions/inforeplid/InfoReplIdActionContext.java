package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.tuple.Pair;

public class InfoReplIdActionContext extends AbstractActionContext<Pair<String, String>, RedisHealthCheckInstance> {

    public InfoReplIdActionContext(RedisHealthCheckInstance instance, Pair<String, String> replIds) {
        super(instance, replIds);
    }

    public InfoReplIdActionContext(RedisHealthCheckInstance instance, Throwable th) {
        super(instance, th);
    }

}
