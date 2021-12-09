package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;

public class RedisMasterActionContext extends AbstractActionContext<Role, RedisHealthCheckInstance> {

    public RedisMasterActionContext(RedisHealthCheckInstance instance, Role role) {
        super(instance, role);
    }

    public RedisMasterActionContext(RedisHealthCheckInstance instance, Throwable th) {
        super(instance, th);
    }

}
