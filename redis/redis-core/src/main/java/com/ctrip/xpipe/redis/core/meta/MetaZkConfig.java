package com.ctrip.xpipe.redis.core.meta;


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

	public static String getZkLeaderLatchRootPath() {
		return System.getProperty("zkLeaderLatchRootPath", "/keepers");
	}
	
	public static String getKeeperLeaderLatchPath(String clusterId, String shardId){
		
		String path = String.format("%s/%s/%s", getZkLeaderLatchRootPath(), clusterId, shardId);
		return path;
	}
	
	public static String getKeeperLeaderElectionId(KeeperMeta currentKeeperMeta){
		
		String leaderElectionID = String.format("%s:%s:%s", currentKeeperMeta.getIp(), currentKeeperMeta.getPort(), currentKeeperMeta.getId());
		return leaderElectionID;
	}
	

	
}
