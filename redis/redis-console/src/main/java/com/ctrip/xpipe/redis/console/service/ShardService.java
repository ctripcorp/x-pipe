package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.ShardTbl;

public interface ShardService {
	ShardTbl find(long shardId);
	ShardTbl find(String clusterName, String shardName);
	List<ShardTbl> findAllByClusterName(String clusterName);
	List<ShardTbl> findAllShardNamesByClusterName(String clusterName);
	ShardTbl createShard(String clusterName, ShardTbl shard);
	void deleteShard(String clusterName, String shardName);
}
