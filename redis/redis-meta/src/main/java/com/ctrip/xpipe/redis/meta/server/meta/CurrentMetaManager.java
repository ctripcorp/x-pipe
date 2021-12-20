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

	Set<Long> allClusters();

	boolean hasCluster(Long clusterDbId);

	boolean hasShard(Long clusterDbId, Long shardDbId);

	void deleteSlot(int slotId);

	void addSlot(int slotId);

	void exportSlot(int slotId);

	void importSlot(int slotId);

	KeeperMeta getKeeperActive(Long clusterDbId, Long shardDbId);

	Pair<String, Integer> getKeeperMaster(Long clusterDbId, Long shardDbId);

	RouteMeta randomRoute(Long clusterDbId);

	RedisMeta getRedisMaster(Long clusterDbId, Long shardDbId);

	ClusterMeta getClusterMeta(Long clusterDbId);

	List<KeeperMeta> getSurviveKeepers(Long clusterDbId, Long shardDbId);

	String getCurrentMetaDesc();

	/************* update support *****************/

	void addResource(Long clusterDbId, Long shardDbId, Releasable releasable);

	void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviceKeepers, KeeperMeta activeKeeper);

	boolean updateKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper);

	boolean watchIfNotWatched(Long clusterDbId, Long shardDbId);

	void setKeeperMaster(Long clusterDbId, Long shardDbId, String addr);

	void setKeeperMaster(Long clusterDbId, Long shardDbId, String ip, int port);

	void setCurrentCRDTMaster(Long clusterDbId, Long shardDbId, long gid, String ip, int port);

	RedisMeta getCurrentCRDTMaster(Long clusterDbId, Long shardDbId);

	RedisMeta getCurrentMaster(Long clusterDbId, Long shardDbId);

	void setPeerMaster(String dcId, Long clusterDbId, Long shardDbId, long gid, String ip, int port);

	RedisMeta getPeerMaster(String dcId, Long clusterDbId, Long shardDbId);

	Set<String> getUpstreamPeerDcs(Long clusterDbId, Long shardDbId);

	Map<String, RedisMeta> getAllPeerMasters(Long clusterDbId, Long shardDbId);

	RouteMeta getClusterRouteByDcId(String dcId, Long clusterDbId);

	void removePeerMaster(String dcId, Long clusterDbId, Long shardDbId);

}
