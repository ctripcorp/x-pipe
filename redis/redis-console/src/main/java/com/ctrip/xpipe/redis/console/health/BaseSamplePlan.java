package com.ctrip.xpipe.redis.console.health;

import java.util.HashMap;
import java.util.Map;

/**
 * @author marsqing
 *
 *         Dec 6, 2016 1:43:52 PM
 */
public abstract class BaseSamplePlan<T> {

	private String clusterId;
	private String shardId;
	private Map<HealthCheckEndpoint, T> hostPort2SampleResult = new HashMap<>();

	public BaseSamplePlan(String clusterId, String shardId) {
		this.clusterId = clusterId;
		this.shardId = shardId;
	}

	public void addRedis(String dcId, HealthCheckEndpoint redisEndpoint, T initSampleResult) {
		hostPort2SampleResult.put(redisEndpoint, initSampleResult);
	}

	public T findInstanceResult(HealthCheckEndpoint endpoint) {
		return hostPort2SampleResult.get(endpoint);
	}

	public Map<HealthCheckEndpoint, T> getHostPort2SampleResult() {
		return hostPort2SampleResult;
	}

	public String getClusterId() {
		return clusterId;
	}

	public String getShardId() {
		return shardId;
	}


	@Override
	public String toString() {
		return String.format("cluster:%s, shard:%s, hosts:%s", clusterId, shardId, hostPort2SampleResult.keySet());
	}

	public boolean isEmpty() {
		return hostPort2SampleResult.isEmpty();
	}
}
