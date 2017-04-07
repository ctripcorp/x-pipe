package com.ctrip.xpipe.redis.console.service;

import java.util.List;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import org.unidal.dal.jdbc.DalException;
import org.unidal.tuple.Pair;

public interface RedisService {
	
	RedisTbl find(long id);
	List<RedisTbl> findAllByDcClusterShard(long dcClusterShardId);
	List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;
	List<RedisTbl> findRedisesByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;
	List<RedisTbl> findKeepersByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;

	void insertRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addr) throws DalException, ResourceNotFoundException;
	void deleteRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addr) throws ResourceNotFoundException;

	void updateByPK(RedisTbl redis);
	void batchUpdate(List<RedisTbl> redises);
	void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel);
	
}
