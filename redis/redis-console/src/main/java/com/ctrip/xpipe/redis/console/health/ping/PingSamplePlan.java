package com.ctrip.xpipe.redis.console.health.ping;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;

/**
 * @author marsqing
 *
 *         Dec 2, 2016 5:58:27 PM
 */
public class PingSamplePlan extends BaseSamplePlan<InstancePingResult> {

	public PingSamplePlan(String clusterId, String shardId) {
		super(clusterId, shardId);
	}

}
