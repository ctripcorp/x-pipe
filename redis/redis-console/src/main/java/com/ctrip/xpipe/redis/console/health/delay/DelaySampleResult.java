package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.health.HealthCheckEndpoint;

import java.util.HashMap;
import java.util.Map;

/**
 * @author marsqing
 *
 *         Dec 2, 2016 11:28:04 AM
 */
public class DelaySampleResult {

	private long sampleStartTime;
	private String clusterId;
	private String shardId;

	private HealthCheckEndpoint masterEndpoint;
	private long masterDelayNanos;

	private Map<HealthCheckEndpoint, Long> slaveHostPort2Delay = new HashMap<>();

	public DelaySampleResult(long sampleStartTime, String clusterId, String shardId) {
		this.sampleStartTime = sampleStartTime;
		this.clusterId = clusterId;
		this.shardId = shardId;
	}

	public void setMasterDelayNanos(HealthCheckEndpoint masterEndpoint, long masterDelayNanos) {
		this.masterEndpoint = masterEndpoint;
		this.masterDelayNanos = masterDelayNanos;
	}

	public void addSlaveDelayNanos(HealthCheckEndpoint slaveHostPort, long slaveDelayNanos) {
		slaveHostPort2Delay.put(slaveHostPort, slaveDelayNanos);
	}

	public long getSampleStartTime() {
		return sampleStartTime;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}

	public HealthCheckEndpoint getMasterEndpoint() {
		return masterEndpoint;
	}

	public long getMasterDelayNanos() {
		return masterDelayNanos;
	}

	public Map<HealthCheckEndpoint, Long> getSlaveHostPort2Delay() {
		return slaveHostPort2Delay;
	}

}
