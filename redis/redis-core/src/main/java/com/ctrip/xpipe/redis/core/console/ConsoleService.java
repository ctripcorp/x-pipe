package com.ctrip.xpipe.redis.core.console;

import java.util.Set;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * all api for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface ConsoleService {
	
	/***********************GET***********************/
	Set<String> getAllDcIds();

	Set<String> getAllClusterIds();

	Set<String> getClusterShardIds(String clusterId);

	DcMeta  getDcMeta(String dcId);

	ClusterMeta getClusterMeta(String dcId, String clusterId);

	ShardMeta getShardMeta(String dcId, String clusterId, String shardId);

	/***********************POST***********************/
	void keeperActiveChanged(String dc, String clusterId, String shardId, KeeperMeta newActiveKeeper);
	
	void redisMasterChanged(String dc, String clusterId, String shardId, RedisMeta newRedisMaster);
	
}
