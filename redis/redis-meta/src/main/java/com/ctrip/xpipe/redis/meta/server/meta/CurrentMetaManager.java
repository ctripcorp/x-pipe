package com.ctrip.xpipe.redis.meta.server.meta;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.*;
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

	ApplierMeta getApplierActive(Long clusterDbId, Long shardDbId);

	Pair<String, Integer> getKeeperMaster(Long clusterDbId, Long shardDbId);

	Pair<String, Integer> getApplierMaster(Long clusterDbId, Long shardDbId);

	RedisMeta getRedisMaster(Long clusterDbId, Long shardDbId);

	ClusterMeta getClusterMeta(Long clusterDbId);

	List<KeeperMeta> getSurviveKeepers(Long clusterDbId, Long shardDbId);

	List<ApplierMeta> getSurviveAppliers(Long clusterDbId, Long shardDbId);

	List<RedisMeta> getRedises(Long clusterDbId, Long shardDbId);

	String getCurrentMetaDesc();

	List<KeeperMeta> getOneWaySurviveKeepers(Long clusterDbId, Long shardDbId);

	/************* update support *****************/

	void setRedises(Long clusterDbId, Long shardDbId, List<RedisMeta> redises);

	void addResource(Long clusterDbId, Long shardDbId, Releasable releasable);

	void setSurviveKeepers(Long clusterDbId, Long shardDbId, List<KeeperMeta> surviceKeepers, KeeperMeta activeKeeper);

	void setSurviveAppliersAndNotify(Long clusterDbId, Long shardDbId, List<ApplierMeta> surviveAppliers, ApplierMeta activeApplier, String sids);

	GtidSet getGtidSet(Long clusterDbId, String srcSids);

	String getSids(Long clusterDbId, Long shardDbId);

	String getSrcSids(Long clusterDbId, Long shardDbId);

	boolean updateKeeperActive(Long clusterDbId, Long shardDbId, KeeperMeta activeKeeper);

	boolean watchKeeperIfNotWatched(Long clusterDbId, Long shardDbId);

	boolean watchApplierIfNotWatched(Long clusterDbId, Long shardDbId);

	void setKeeperMaster(Long clusterDbId, Long shardDbId, String addr);

	void setKeeperMaster(Long clusterDbId, Long shardDbId, String ip, int port);

	void setApplierMasterAndNotify(Long clusterDbId, Long shardDbId, String ip, int port, String sids);

	void setSrcSidsAndNotify(Long clusterDbId, Long shardDbId, String sids);

	void setCurrentCRDTMaster(Long clusterDbId, Long shardDbId, long gid, String ip, int port);

	RedisMeta getCurrentCRDTMaster(Long clusterDbId, Long shardDbId);

	RedisMeta getCurrentMaster(Long clusterDbId, Long shardDbId);

	void setPeerMaster(String dcId, Long clusterDbId, Long shardDbId, long gid, String ip, int port);

	RedisMeta getPeerMaster(String dcId, Long clusterDbId, Long shardDbId);

	Set<String> getUpstreamPeerDcs(Long clusterDbId, Long shardDbId);

	Map<String, RedisMeta> getAllPeerMasters(Long clusterDbId, Long shardDbId);

	RouteMeta getClusterRouteByDcId(String dstDcId, Long clusterDbId);

	void removePeerMaster(String dcId, Long clusterDbId, Long shardDbId);

}
