package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;

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

	private HostPort masterHostPort;
	private long masterDelayNanos;

	private Map<HostPort, Long> slaveHostPort2Delay = new HashMap<>();

	public DelaySampleResult(long sampleStartTime, String clusterId, String shardId) {
		this.sampleStartTime = sampleStartTime;
		this.clusterId = clusterId;
		this.shardId = shardId;
	}

	public void setMasterDelayNanos(HostPort masterHostPort, long masterDelayNanos) {
		this.masterHostPort = masterHostPort;
		this.masterDelayNanos = masterDelayNanos;
	}

	public void addSlaveDelayNanos(HostPort slaveHostPort, long slaveDelayNanos) {
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

	public HostPort getMasterHostPort() {
		return masterHostPort;
	}

	public long getMasterDelayNanos() {
		return masterDelayNanos;
	}

	public Map<HostPort, Long> getSlaveHostPort2Delay() {
		return slaveHostPort2Delay;
	}

}
