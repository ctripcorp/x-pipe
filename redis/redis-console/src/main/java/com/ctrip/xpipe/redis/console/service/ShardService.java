package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterModel;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.ShardListModel;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;

public interface ShardService {
	ShardTbl find(long shardId);
	ShardTbl find(String clusterName, String shardName);
	List<ShardTbl> findAllByClusterName(String clusterName);
	List<ShardTbl> findAllShardNamesByClusterName(String clusterName);
	ShardTbl createShard(String clusterName, ShardTbl shard, Map<Long, SentinelGroupModel> sentinels);
    ShardTbl findOrCreateShardIfNotExist(String clusterName, ShardTbl shard, Map<Long, SentinelGroupModel> sentinels);
	void deleteShard(String clusterName, String shardName);
	void deleteShards(ClusterTbl cluster, List<String> shardNames);
	List<ShardListModel> findAllUnhealthy();
    void updateShardsByDcClusterModel(DcClusterModel dcClusterModel, ClusterTbl clusterTbl) throws DalException;
    List<ShardTbl> findAllShardByDcCluster(long dcId, long clusterId);
}
