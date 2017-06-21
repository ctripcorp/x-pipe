package com.ctrip.xpipe.redis.core.meta;


import java.util.List;
import java.util.Set;

import com.ctrip.xpipe.metric.HostPort;
import org.unidal.tuple.Pair;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.MetaServerMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.ZkServerMeta;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface XpipeMetaManager extends MetaUpdateOperation{
	
	boolean dcExists(String dc);
	
	Set<String> getDcs();
	
	Set<String> getDcClusters(String dc);
	
	ClusterMeta getClusterMeta(String dc, String clusterId);
	
	ShardMeta getShardMeta(String dc, String clusterId, String shardId);

	List<KeeperMeta> getKeepers(String dc, String clusterId, String shardId);

	List<RedisMeta> getRedises(String dc, String clusterId, String shardId);

	KeeperMeta getKeeperActive(String dc, String clusterId, String shardId);
	
	List<KeeperMeta> getKeeperBackup(String dc, String clusterId, String shardId);

	ShardMeta findShardMeta(HostPort hostPort);
	
	/**
	 * @param clusterId
	 * @param shardId
	 * @return dc and redismeta info
	 */
	Pair<String, RedisMeta> getRedisMaster(String clusterId, String shardId);
	
	List<MetaServerMeta> getMetaServers(String dc);
	
	SentinelMeta getSentinel(String dc, String clusterId, String shardId);
	
	ZkServerMeta  getZkServerMeta(String dc);

	String getActiveDc(String clusterId, String shardId) throws MetaException;
	
	Set<String> getBackupDcs(String clusterId, String shardId);

	KeeperContainerMeta getKeeperContainer(String dc, KeeperMeta keeperMeta);

	DcMeta getDcMeta(String dc);

	List<KeeperMeta> getAllSurviceKeepers(String currentDc, String clusterId, String shardId);

	boolean hasCluster(String currentDc, String clusterId);

	boolean hasShard(String currentDc, String clusterId, String shardId);

	void primaryDcChanged(String currentDc, String clusterId, String shardId, String newPrimaryDc);

}
