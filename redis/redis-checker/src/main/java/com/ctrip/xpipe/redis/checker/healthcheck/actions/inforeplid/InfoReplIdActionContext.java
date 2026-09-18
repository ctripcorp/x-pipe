package com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.unidal.tuple.Triple;

public class InfoReplIdActionContext extends AbstractActionContext<Triple<String, String, String>, RedisHealthCheckInstance> {

    public InfoReplIdActionContext(RedisHealthCheckInstance instance, Triple<String, String, String> replIds) {
        super(instance, replIds);
    }

    public InfoReplIdActionContext(RedisHealthCheckInstance instance, Throwable th) {
        super(instance, th);
    }

}
