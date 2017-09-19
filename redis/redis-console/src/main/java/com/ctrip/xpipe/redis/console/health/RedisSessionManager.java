package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Set;

/**
 * @author marsqing
 *
 * Dec 1, 2016 6:40:43 PM
 */
public interface RedisSessionManager {

	RedisSession findOrCreateSession(String host, int port);
}
