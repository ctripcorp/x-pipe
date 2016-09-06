package com.ctrip.xpipe.redis.core.meta;

import java.util.List;
import java.util.Set;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface DcMetaManager{
	
	Set<String> getClusters();
	
	boolean hasCluster(String clusterId);
	
	boolean hasShard(String clusterId, String shardId);
	
	ClusterMeta getClusterMeta(String clusterId);
	
	ShardMeta getShardMeta(String clusterId, String shardId);

	List<KeeperMeta> getKeepers(String clusterId, String shardId);

	List<RedisMeta> getRedises(String clusterId, String shardId);

	KeeperMeta getKeeperActive(String clusterId, String shardId);
	
	List<KeeperMeta> getKeeperBackup(String clusterId, String shardId);
	
	/**
	 * @param clusterId
	 * @param shardId
	 * @return dc and redismeta info
	 */
	RedisMeta getRedisMaster(String clusterId, String shardId);
	
	List<MetaServerMeta> getMetaServers();
	
	ZkServerMeta  getZkServerMeta();

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	String getUpstream(String clusterId, String shardId) throws MetaException;
	
	DcMeta getDcMeta();
	
	void update(ClusterMeta clusterMeta);

	ClusterMeta removeCluster(String clusterId);

	boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper);
	
	boolean noneKeeperActive(String clusterId, String shardId);
	
	boolean updateRedisMaster(String clusterId, String shardId, RedisMeta redisMaster);

	List<KeeperMeta> getAllSurviveKeepers(String clusterId, String shardId);

	void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviceKeepers);

	
}
