package com.ctrip.xpipe.redis.console.health.ping;

import java.util.HashMap;
import java.util.Map;

import com.ctrip.xpipe.metric.HostPort;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 8:03:04 PM
 */
public class PingSampleResult {

	private String clusterId;
	private String shardId;
	private Map<HostPort, Boolean> slaveHostPort2Pong = new HashMap<>();

	public PingSampleResult(String clusterId, String shardId) {
		this.clusterId = clusterId;
		this.shardId = shardId;
	}

	public void addPong(HostPort hostPort, InstancePingResult pingResult) {
		slaveHostPort2Pong.put(hostPort, pingResult.isSuccess());
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}

	public Map<HostPort, Boolean> getSlaveHostPort2Pong() {
		return slaveHostPort2Pong;
	}

}
