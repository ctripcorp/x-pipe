package com.ctrip.xpipe.redis.core.meta;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;

/**
 * meta related zk config information
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class MetaZkConfig {
	
	public static String  getMetaRootPath(){
		return System.getProperty("zkMetaStoragePath", "/meta");
	}
	
	public static String getMetaServerLeaderElectPath(){
		return System.getProperty("zkMetaStoragePath", "/metaserver/leader");
	}

	public static String getMetaServerRegisterPath(){
		return System.getProperty("zkMetaStoragePath", "/metaserver/servers");
	}

	public static String getMetaServerSlotsPath(){
		return System.getProperty("zkMetaStoragePath", "/metaserver/slots");
	}

	public static String getZkLeaderLatchRootPath() {
		return System.getProperty("zkLeaderLatchRootPath", "/keepers");
	}

	public static String getApplierZkLeaderLatchRootPath() {
		return System.getProperty("applierZkLeaderLatchRootPath", "/appliers");
	}

	public static String getKeeperLeaderLatchPath(ClusterId clusterId, ShardId shardId){
		return getKeeperLeaderLatchPath(clusterId.toString(), shardId.toString());
	}
	
	public static String getKeeperLeaderLatchPath(String clusterId, String shardId){
		
		String path = String.format("%s/%s/%s", getZkLeaderLatchRootPath(), clusterId, shardId);
		return path;
	}

	public static String getApplierLeaderLatchPath(ClusterId clusterId, ShardId shardId){
		return getApplierLeaderLatchPath(clusterId.toString(), shardId.toString());
	}

	public static String getApplierLeaderLatchPath(String clusterId, String shardId){

		String path = String.format("%s/%s/%s", getApplierZkLeaderLatchRootPath(), clusterId, shardId);
		return path;
	}

	public static String getKeeperLeaderLatchPath(long clusterDbId, long shardDbId) {
		return String.format("%s/cluster_%d/shard_%d", getZkLeaderLatchRootPath(), clusterDbId, shardDbId);
	}

	public static String getApplierLeaderLatchPath(long clusterDbId, long shardDbId) {
		return String.format("%s/cluster_%d/shard_%d", getApplierZkLeaderLatchRootPath(), clusterDbId, shardDbId);
	}

	public static String getKeeperLeaderElectionId(KeeperMeta currentKeeperMeta){
		
		String leaderElectionID = Codec.DEFAULT.encode(currentKeeperMeta);
		return leaderElectionID;
	}

	public static String getApplierLeaderElectionId(ApplierMeta currentApplierMeta){

		String leaderElectionID = Codec.DEFAULT.encode(currentApplierMeta);
		return leaderElectionID;
	}
}
