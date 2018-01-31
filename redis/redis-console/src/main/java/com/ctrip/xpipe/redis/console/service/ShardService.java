package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;

public interface ShardService {
	ShardTbl find(long shardId);
	ShardTbl find(String clusterName, String shardName);
	List<ShardTbl> findAllByClusterName(String clusterName);
	List<ShardTbl> findAllShardNamesByClusterName(String clusterName);
	ShardTbl createShard(String clusterName, ShardTbl shard, Map<Long, SetinelTbl> sentinels);
	ShardTbl findOrCreateShardIfNotExist(String clusterName, ShardTbl shard, Map<Long, SetinelTbl> sentinels);
	void deleteShard(String clusterName, String shardName);
}
