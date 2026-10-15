package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import org.unidal.dal.jdbc.DalException;

import java.util.List;
import java.util.Map;

public interface RedisService {
	
	RedisTbl find(long id);

	RedisTbl findByIpPort(String ip, int port);

	List<RedisTbl> findAllRedisWithSameIP(String ip);

	/**
	 * 按 pod hostname 查同一容器上的实例。容器 keeper 的 redis_ip 统一是 0.0.0.0，
	 * 用 findAllRedisWithSameIP 会把所有 pod 归并成同一台机器，端口记账因此必须换键。
	 */
	List<RedisTbl> findAllRedisWithSameHostName(String hostName);

	/**
	 * 按 keepercontainer_id 查询该容器上的所有 keeper 实例端口
	 */
	List<RedisTbl> findAllKeepersByKeeperContainerId(long keeperContainerId);

	List<RedisTbl> findAllRedisByIp(String ip);

	List<RedisTbl> findAllByDcClusterShard(long dcClusterShardId);

	List<RedisTbl> findAllRedisesByDcClusterName(String dcId, String clusterId);

	List<RedisTbl> findAllKeepersByDcClusterName(String dcId, String clusterId);

	List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;

	List<RedisTbl> findRedisesByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;
	List<RedisTbl> findKeepersByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException;

	void insertRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) throws DalException, ResourceNotFoundException;
	void insertRedises(String dcId, String clusterId, String shardId, Map<Pair<String, Integer>, String> addrToAzName) throws DalException, ResourceNotFoundException;
	void deleteRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> addrs) throws ResourceNotFoundException;

	int insertKeepers(String dcId, String clusterId, String shardId, List<KeeperBasicInfo> keepers) throws DalException, ResourceNotFoundException;
	List<RedisTbl> deleteKeepers(String dcId, String clusterId, String shardId) throws DalException, ResourceNotFoundException;


	void updateByPK(RedisTbl redis);
	void updateBatchMaster(List<RedisTbl> redises);
	void updateBatchKeeperActive(List<RedisTbl> redises);

    void updateSourceKeepers(String srcDcName, String clusterName, String shardName, long dstDcId, ShardModel sourceShard);

    void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel);

	void updateRedisesAz(String dcId, String clusterId, String shardId, Map<String, String> addressAzNameMap) throws ResourceNotFoundException;

	List<RedisTbl> findAllKeeperContainerCountInfo();

	List<Long> findClusterIdsByKeeperContainer(long keeperContainerId);
}
