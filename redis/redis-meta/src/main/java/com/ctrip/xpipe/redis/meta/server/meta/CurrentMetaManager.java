package com.ctrip.xpipe.redis.meta.server.meta;


import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public interface CurrentMetaManager extends Observable{
	
	Set<String> allClusters();
	
	boolean hasCluster(String clusterId);
	
	boolean hasShard(String clusterId, String shardId);
	
	void deleteSlot(int slotId);
	
	void addSlot(int slotId);

	void exportSlot(int slotId);

	void importSlot(int slotId);
	
	KeeperMeta getKeeperActive(String clusterId, String shardId);

	InetSocketAddress  getKeeperMaster(String clusterId, String shardId);
	
	RedisMeta getRedisMaster(String clusterId, String shardId);

	String getUpstream(String clusterId, String shardId);

	List<KeeperMeta> getKeepers(String clusterId, String shardId);

	ClusterMeta getClusterMeta(String clusterId);

	List<KeeperMeta> getSurviveKeepers(String clusterId, String shardId);

	String getCurrentMetaDesc();
	
	/*************update support*****************/
	
	void addResource(String clusterId, String shardId, Releasable releasable);
	
	void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviceKeepers, KeeperMeta activeKeeper);
	
	boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper);

	boolean watchIfNotWatched(String clusterId, String shardId);
	
	void  	setKeeperMaster(String cluserId, String shardId, String addr);
}
