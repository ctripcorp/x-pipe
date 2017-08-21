package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author shyin
 *
 * Jan 5, 2017
 */
public interface DelayService {

	long getDelay(HostPort hostPort);
	
}
