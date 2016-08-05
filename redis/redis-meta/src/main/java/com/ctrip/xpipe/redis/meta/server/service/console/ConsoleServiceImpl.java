package com.ctrip.xpipe.redis.meta.server.service.console;

import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@Service
public class ConsoleServiceImpl implements ConsoleService {

	@Autowired
	private RetryableRestTemplate restTemplate;

	@Override
	public Set<String> getAllDcIds() {
		String[] dcIds = restTemplate.get("/api/dcids", String[].class);
		if (dcIds == null || dcIds.length == 0){
			return Collections.EMPTY_SET;
		}
		return new HashSet<>(Arrays.asList(dcIds));

	}

	@Override
	public Set<String> getAllClusterIds() {
		String[] clustersIds = restTemplate.get("/api/clusterids", String[].class);
		if (clustersIds == null || clustersIds.length == 0){
			return Collections.EMPTY_SET;
		}
		return new HashSet<>(Arrays.asList(clustersIds));
	}

	@Override
	public Set<String> getClusterShardIds(String clusterId) {
		String[] shardIds = restTemplate.get("/api/cluster/{clusterId}/shardids", String[].class, clusterId);
		if (shardIds == null || shardIds.length == 0){
			return Collections.EMPTY_SET;
		}
		return new HashSet<>(Arrays.asList(shardIds));
	}

	@Override
	public DcMeta getDcMeta(String dcId) {
		return restTemplate.get("/api/dc/{dcId}", DcMeta.class, dcId);
	}

	@Override
	public ClusterMeta getClusterMeta(String dcId, String clusterId) {
		return restTemplate.get("/api/dc/{dcId}/cluster/{clusterId}", ClusterMeta.class, dcId, clusterId);
	}

	@Override
	public ShardMeta getShardMeta(String dcId, String clusterId, String shardId) {
		return restTemplate
			.get("/api/dc/{dcId}/cluster/{clusterId}/shard/{shardId}", ShardMeta.class, dcId, clusterId, shardId);
	}

	@Override
	public void keeperActiveChanged(String dc, String clusterId, String shardId, KeeperMeta newActiveKeeper)
		throws Exception {

	}

	@Override
	public void redisMasterChanged(String dc, String clusterId, String shardId, RedisMeta newRedisMaster)
		throws Exception {

	}
}
