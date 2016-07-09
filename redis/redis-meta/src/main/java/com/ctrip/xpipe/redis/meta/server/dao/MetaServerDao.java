package com.ctrip.xpipe.redis.meta.server.dao;

import java.util.List;
import java.util.Set;

import com.ctrip.xpipe.redis.core.dao.DaoException;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
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
public interface MetaServerDao extends MetaServerUpdateOperation{
	
	Set<String> getClusters();
	
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
	
	String getUpstream(String clusterId, String shardId) throws DaoException;

}
