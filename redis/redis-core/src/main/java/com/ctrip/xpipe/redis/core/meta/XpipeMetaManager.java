package com.ctrip.xpipe.redis.core.meta;


import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface XpipeMetaManager extends MetaUpdateOperation{


	public static class MetaDesc extends ShardMeta {

		private DcMeta dcMeta;
		private ClusterMeta clusterMeta;
		private ShardMeta shardMeta;
		private Redis redis;

		public MetaDesc(DcMeta dcMeta, ClusterMeta clusterMeta, ShardMeta shardMeta, Redis redis){
			this.dcMeta = dcMeta;
			this.clusterMeta = clusterMeta;
			this.shardMeta = shardMeta;
			this.redis = redis;
		}

		public String getDcId() {
			return dcMeta != null ? dcMeta.getId() : null;
		}

		public String getClusterId() {
			return clusterMeta != null ? clusterMeta.getId() : null;
		}

		public String getShardId() {
			return shardMeta != null ? shardMeta.getId() : null;
		}

		public String getActiveDc(){
			return shardMeta != null ? shardMeta.getActiveDc() : null;
		}

		public Redis getRedis() {
			return redis;
		}
	}
	
	boolean dcExists(String dc);
	
	Set<String> getDcs();
	
	Set<String> getDcClusters(String dc);
	
	ClusterMeta getClusterMeta(String dc, String clusterId);
	
	ShardMeta getShardMeta(String dc, String clusterId, String shardId);

	List<KeeperMeta> getKeepers(String dc, String clusterId, String shardId);

	List<RedisMeta> getRedises(String dc, String clusterId, String shardId);

	KeeperMeta getKeeperActive(String dc, String clusterId, String shardId);
	
	List<KeeperMeta> getKeeperBackup(String dc, String clusterId, String shardId);

	MetaDesc findMetaDesc(HostPort hostPort);
	
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
