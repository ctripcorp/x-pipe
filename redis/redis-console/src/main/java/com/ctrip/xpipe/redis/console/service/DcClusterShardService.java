package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Set;

public interface DcClusterShardService {

	DcClusterShardTbl find(long dcClusterId, long shardId);
	DcClusterShardTbl find(String dcName, String clusterName, String shardName);
	List<DcClusterShardTbl> findAllByDcCluster(long dcClusterId);
	List<DcClusterShardTbl> findAllByDcCluster(String dcName, String clusterName);
	void updateDcClusterShard(DcClusterShardTbl dcClusterShardTbl) throws DalException;
	List<DcClusterShardTbl> findAllByDcId(long dcId);
	List<DcClusterShardTbl> findAllByDcIdAndInClusterTypes(long dcId, Set<String> clusterTypes);
}
