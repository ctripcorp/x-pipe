package com.ctrip.xpipe.redis.console.health.ping;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.HealthCheckEndpoint;

import java.util.HashMap;
import java.util.Map;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 8:03:04 PM
 */
public class PingSampleResult {

	private String clusterId;
	private String shardId;
	private Map<HealthCheckEndpoint, Boolean> slaveHostPort2Pong = new HashMap<>();

	public PingSampleResult(String clusterId, String shardId) {
		this.clusterId = clusterId;
		this.shardId = shardId;
	}

	public void addPong(HealthCheckEndpoint endpoint, InstancePingResult pingResult) {
		slaveHostPort2Pong.put(endpoint, pingResult.isSuccess());
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}

	public Map<HealthCheckEndpoint, Boolean> getSlaveHostPort2Pong() {
		return slaveHostPort2Pong;
	}

}
