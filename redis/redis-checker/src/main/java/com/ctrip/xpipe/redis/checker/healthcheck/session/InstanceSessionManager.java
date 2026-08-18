package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author yu
 * <p>
 * 2023/10/31
 */
public interface InstanceSessionManager {

    RedisSession findOrCreateSession(Endpoint endpoint);

    RedisSession findOrCreateSession(HostPort hostPort);
}
