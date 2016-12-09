package com.ctrip.xpipe.redis.console.health.delay;

/**
 * @author marsqing
 *
 * Dec 2, 2016 11:37:23 AM
 */
public interface DelayCollector {

	void collect(DelaySampleResult result);

}
