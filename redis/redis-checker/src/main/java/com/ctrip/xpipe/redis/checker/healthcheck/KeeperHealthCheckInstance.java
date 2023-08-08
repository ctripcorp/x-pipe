package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;

public interface KeeperHealthCheckInstance extends HealthCheckInstance<KeeperInstanceInfo>{

    Endpoint getEndpoint();

    RedisSession getRedisSession();
}
