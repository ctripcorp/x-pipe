package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

public class RedisMasterActionContext extends AbstractActionContext<Server.SERVER_ROLE> {

    public RedisMasterActionContext(RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {
        super(instance, role);
    }

}
