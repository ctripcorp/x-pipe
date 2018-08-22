package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.DefaultHealthCheckEndpoint;
import com.ctrip.xpipe.redis.console.health.HealthCheckEndpoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 2:17:20 PM
 */
public class DelaySamplePlan extends BaseSamplePlan<InstanceDelayResult> {

	private String masterDcId;
	private HealthCheckEndpoint masterEndpoint;

	public DelaySamplePlan(String clusterId, String shardId) {
		super(clusterId, shardId);
	}

	public void addRedis(String dcId, HealthCheckEndpoint endpoint) {
		boolean isMaster = endpoint.getRedisMeta().isMaster();
		if (isMaster) {
			masterDcId = dcId;
			masterEndpoint = endpoint;
		}

		super.addRedis(dcId, endpoint, new InstanceDelayResult(dcId, isMaster));
	}

	public void addRedis(String dcId, RedisMeta redisMeta) {
		boolean isMaster = redisMeta.isMaster();
		HealthCheckEndpoint endpoint = new DefaultHealthCheckEndpoint(redisMeta);
		if (isMaster) {
			masterDcId = dcId;
			masterEndpoint = endpoint;
		}

		super.addRedis(dcId, endpoint, new InstanceDelayResult(dcId, isMaster));
	}

	public HealthCheckEndpoint getMasterEndpoint() {
		return masterEndpoint;
	}

	public String getMasterDcId() {
		return masterDcId;
	}

}
