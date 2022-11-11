package com.ctrip.xpipe.redis.core.console;

import com.ctrip.xpipe.redis.core.entity.*;

import java.util.Set;

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

	DcMeta getDcMeta(String dcId, Set<String> types);

	ClusterMeta getClusterMeta(String dcId, String clusterId);

	ShardMeta getShardMeta(String dcId, String clusterId, String shardId);

	/***********************POST***********************/
	void keeperActiveChanged(String dc, String clusterId, String shardId, KeeperMeta newActiveKeeper);

	void applierActiveChanged(String dc, String clusterId, String shardId, ApplierMeta newActiveApplier);
	
	void redisMasterChanged(String dc, String clusterId, String shardId, RedisMeta newRedisMaster);
	
}
