package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 *
 *         Aug 6, 2016
 */
public interface CurrentMetaManager extends Observable {

	Set<String> allClusters();

	boolean hasCluster(String clusterId);

	boolean hasShard(String clusterId, String shardId);

	void deleteSlot(int slotId);

	void addSlot(int slotId);

	void exportSlot(int slotId);

	void importSlot(int slotId);

	KeeperMeta getKeeperActive(String clusterId, String shardId);

	Pair<String, Integer> getKeeperMaster(String clusterId, String shardId);

	RouteMeta randomRoute(String clusterId);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	ClusterMeta getClusterMeta(String clusterId);

	List<KeeperMeta> getSurviveKeepers(String clusterId, String shardId);

	String getCurrentMetaDesc();

	/************* update support *****************/

	void addResource(String clusterId, String shardId, Releasable releasable);

	void setSurviveKeepers(String clusterId, String shardId, List<KeeperMeta> surviceKeepers, KeeperMeta activeKeeper);

	boolean updateKeeperActive(String clusterId, String shardId, KeeperMeta activeKeeper);

	boolean watchIfNotWatched(String clusterId, String shardId);

	void setKeeperMaster(String clusterId, String shardId, String addr);

	void setKeeperMaster(String clusterId, String shardId, String ip, int port);

	void setCurrentCRDTMaster(String clusterId, String shardId, long gid, String ip, int port);

	RedisMeta getCurrentCRDTMaster(String clusterId, String shardId);

	RedisMeta getCurrentMaster(String clusterId, String shardId);

	void setPeerMaster(String dcId, String clusterId, String shardId, long gid, String ip, int port);

	RedisMeta getPeerMaster(String dcId, String clusterId, String shardId);

	Set<String> getUpstreamPeerDcs(String clusterId, String shardId);

	Map<String, RedisMeta> getAllPeerMasters(String clusterId, String shardId);

	RouteMeta getClusterRouteByDcId(String dcId, String clusterId);

	void removePeerMaster(String dcId, String clusterId, String shardId);

}
