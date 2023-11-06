package com.ctrip.xpipe.redis.checker.healthcheck.session;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author marsqing
 *
 * Dec 1, 2016 6:40:43 PM
 */
public interface RedisSessionManager {

	RedisSession findOrCreateSession(Endpoint endpoint);

	RedisSession findOrCreateSession(HostPort hostPort);
}
