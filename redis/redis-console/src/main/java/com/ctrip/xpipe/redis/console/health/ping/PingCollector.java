package com.ctrip.xpipe.redis.console.health.ping;

/**
 * @author marsqing
 *
 * Dec 6, 2016 6:36:09 PM
 */
public interface PingCollector {

	void collect(PingSampleResult result);
	
}
