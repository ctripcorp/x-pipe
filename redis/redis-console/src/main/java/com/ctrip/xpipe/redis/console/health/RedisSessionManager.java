package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.api.endpoint.Endpoint;

/**
 * @author marsqing
 *
 * Dec 1, 2016 6:40:43 PM
 */
public interface RedisSessionManager {

	RedisSession findOrCreateSession(String host, int port);

	RedisSession findOrCreateSession(Endpoint endpoint);
}
