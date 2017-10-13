package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

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
	private Map<HostPort, T> hostPort2SampleResult = new HashMap<>();

	public BaseSamplePlan(String clusterId, String shardId) {
		this.clusterId = clusterId;
		this.shardId = shardId;
	}

	public void addRedis(String dcId, RedisMeta redisMeta, T initSampleResult) {
		hostPort2SampleResult.put(new HostPort(redisMeta.getIp(), redisMeta.getPort()), initSampleResult);
	}

	public T findInstanceResult(HostPort hostPort) {
		return hostPort2SampleResult.get(hostPort);
	}

	public Map<HostPort, T> getHostPort2SampleResult() {
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
}
