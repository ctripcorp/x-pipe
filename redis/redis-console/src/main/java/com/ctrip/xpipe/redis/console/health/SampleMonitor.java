package com.ctrip.xpipe.redis.console.health;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 5:05:47 PM
 */
public interface SampleMonitor<T> {

	void startSample(BaseSamplePlan<T> plan) throws Exception;

}
