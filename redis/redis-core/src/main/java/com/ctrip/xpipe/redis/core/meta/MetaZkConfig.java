package com.ctrip.xpipe.redis.core.meta;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

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
	
	public static String getKeeperLeaderLatchPath(String clusterId, String shardId){
		
		String path = String.format("%s/%s/%s", getZkLeaderLatchRootPath(), clusterId, shardId);
		return path;
	}

	public static String getKeeperLeaderLatchPath(long clusterDbId, long shardDbId) {
		return String.format("%s/cluster_%d/shard_%d", getZkLeaderLatchRootPath(), clusterDbId, shardDbId);
	}
	
	public static String getKeeperLeaderElectionId(KeeperMeta currentKeeperMeta){
		
		String leaderElectionID = Codec.DEFAULT.encode(currentKeeperMeta);
		return leaderElectionID;
	}
	

	
}
