package com.ctrip.xpipe.redis.core.dao;


import java.util.List;
import java.util.Set;

import org.unidal.tuple.Pair;

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
public interface MetaDao extends MetaUpdateOperation{
	
	boolean dcExists(String dc);
	
	Set<String> getDcs();
	
	Set<String> getDcClusters(String dc);
	
	ClusterMeta getClusterMeta(String dc, String clusterId);
	
	ShardMeta getShardMeta(String dc, String clusterId, String shardId);

	List<KeeperMeta> getKeepers(String dc, String clusterId, String shardId);

	List<RedisMeta> getRedises(String dc, String clusterId, String shardId);

	KeeperMeta getKeeperActive(String dc, String clusterId, String shardId);
	
	List<KeeperMeta> getKeeperBackup(String dc, String clusterId, String shardId);
	
	/**
	 * @param clusterId
	 * @param shardId
	 * @return dc and redismeta info
	 */
	Pair<String, RedisMeta> getRedisMaster(String clusterId, String shardId);
	
	List<MetaServerMeta> getMetaServers(String dc);
	
	ZkServerMeta  getZkServerMeta(String dc);

	String getActiveDc(String clusterId) throws DaoException;

	List<String> getBackupDc(String clusterId) throws DaoException;

	boolean updateUpstreamKeeper(String dc, String clusterId, String shardId, String address) throws DaoException;

	String getUpstream(String dc, String clusterId, String shardId) throws DaoException;
}
