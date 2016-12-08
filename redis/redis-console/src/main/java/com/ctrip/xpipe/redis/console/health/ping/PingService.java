package com.ctrip.xpipe.redis.console.health.ping;

import com.ctrip.xpipe.metric.HostPort;

/**
 * @author marsqing
 *
 * Dec 8, 2016 11:46:39 AM
 */
public interface PingService {

	boolean isRedisAlive(HostPort hostPort);

}
