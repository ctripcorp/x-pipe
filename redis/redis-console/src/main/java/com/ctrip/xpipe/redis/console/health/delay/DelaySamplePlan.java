package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author marsqing
 *
 *         Dec 1, 2016 2:17:20 PM
 */
public class DelaySamplePlan extends BaseSamplePlan<InstanceDelayResult> {

	private String masterDcId;
	private String masterHost;
	private int masterPort;

	public DelaySamplePlan(String clusterId, String shardId) {
		super(clusterId, shardId);
	}

	public void addRedis(String dcId, RedisMeta redisMeta) {
		if (redisMeta.isMaster()) {
			masterDcId = dcId;
			masterHost = redisMeta.getIp();
			masterPort = redisMeta.getPort();
		}

		super.addRedis(dcId, redisMeta, new InstanceDelayResult(dcId, redisMeta.isMaster()));
	}

	public String getMasterHost() {
		return masterHost;
	}

	public int getMasterPort() {
		return masterPort;
	}

	public String getMasterDcId() {
		return masterDcId;
	}

}
