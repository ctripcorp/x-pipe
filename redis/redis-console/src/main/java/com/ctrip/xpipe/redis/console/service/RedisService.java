package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import org.unidal.dal.jdbc.DalException;

import java.util.List;

public interface RedisService {
	
	RedisTbl find(long id);

	RedisTbl findByIpPort(String ip, int port);

	List<RedisTbl> findAllRedisWithSameIP(String ip);

	List<RedisTbl> findAllByDcClusterShard(long dcClusterShardId);

	List<RedisTbl> findAllRedisesByDcClusterName(String dcId, String clusterId);
	List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;

	List<RedisTbl> findRedisesByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;
	List<RedisTbl> findKeepersByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;

	void insertRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) throws DalException, ResourceNotFoundException;
	void deleteRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) throws ResourceNotFoundException;

	int insertKeepers(String dcId, String clusterId, String shardId, List<KeeperBasicInfo> keepers) throws DalException, ResourceNotFoundException;
	List<RedisTbl> deleteKeepers(String dcId, String clusterId, String shardId) throws DalException, ResourceNotFoundException;


	void updateByPK(RedisTbl redis);
	void updateBatchMaster(List<RedisTbl> redises);
	void updateBatchKeeperActive(List<RedisTbl> redises);

	void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel);

	List<RedisTbl> findAllKeeperContainerCountInfo();

	List<Long> findClusterIdsByKeeperContainer(long keeperContainerId);
}
