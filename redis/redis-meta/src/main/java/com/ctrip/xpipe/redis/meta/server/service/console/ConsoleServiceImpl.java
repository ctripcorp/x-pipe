package com.ctrip.xpipe.redis.meta.server.service.console;

import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.dto.DCDTO;
import com.ctrip.xpipe.utils.BeanUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

@Service
public class ConsoleServiceImpl implements ConsoleService {

	@Autowired
	private RetryableRestTemplate restTemplate;

	@Override
	public Set<String> getAllDcIds() {
		DCDTO[] dcs = restTemplate.get("/api/dcs", DCDTO[].class);
		if (dcs == null || dcs.length == 0) {
			return Collections.emptySet();
		}
		return BeanUtils.toPropertySet("dcName", Arrays.asList(dcs));
	}

	@Override
	public Set<String> getAllClusterIds() {
		return null;
	}

	@Override
	public Set<String> getClusterShardIds(String clusterId) {
		return null;
	}

	@Override
	public DcMeta getDcMeta(String dcId) {
		return null;
	}

	@Override
	public ClusterMeta getClusterMeta(String dcId, String clusterId) {
		return restTemplate.get("/api/dc/{dcId}/cluster/{clusterId}", ClusterMeta.class, dcId, clusterId);
	}

	@Override
	public ShardMeta getShardMeta(String dcId, String clusterId, String shardId) {
		return null;
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
