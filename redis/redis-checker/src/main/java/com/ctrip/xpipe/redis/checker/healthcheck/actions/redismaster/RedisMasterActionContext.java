package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

public class RedisMasterActionContext extends AbstractActionContext<Server.SERVER_ROLE, RedisHealthCheckInstance> {

    public RedisMasterActionContext(RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {
        super(instance, role);
    }

}
