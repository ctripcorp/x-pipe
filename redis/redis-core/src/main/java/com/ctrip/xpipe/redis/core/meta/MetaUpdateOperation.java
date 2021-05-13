package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;

import java.util.List;

/**
 * @author wenchao.meng
 *
 * Jul 7, 2016
 */
public interface MetaUpdateOperation extends ReadWriteSafe {

	default boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper) { return write(()->doUpdateKeeperActive(dc, clusterId, shardId, activeKeeper)); }
	boolean doUpdateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper);

	default boolean noneKeeperActive(String currentDc, String clusterId, String shardId) { return write(()->doNoneKeeperActive(currentDc, clusterId, shardId)); }
	boolean doNoneKeeperActive(String currentDc, String clusterId, String shardId);

	default boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster) { return write(()->doUpdateRedisMaster(dc, clusterId, shardId, redisMaster)); }
	boolean doUpdateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster);

	default void update(DcMeta dcMeta) { write(()->doUpdate(dcMeta)); }
	void doUpdate(DcMeta dcMeta);

	default void update(String dcId, ClusterMeta clusterMeta) { write(()->doUpdate(dcId, clusterMeta)); }
	void doUpdate(String dcId, ClusterMeta clusterMeta);

	default ClusterMeta removeCluster(String currentDc, String clusterId) { return write(()->doRemoveCluster(currentDc, clusterId)); }
	ClusterMeta doRemoveCluster(String currentDc, String clusterId);

	default void setSurviveKeepers(String dcId, String clusterId, String shardId, List<KeeperMeta> surviceKeepers) { write(()->doSetSurviveKeepers(dcId, clusterId, shardId, surviceKeepers)); }
	void doSetSurviveKeepers(String dcId, String clusterId, String shardId, List<KeeperMeta> surviceKeepers);

	default void primaryDcChanged(String currentDc, String clusterId, String shardId, String newPrimaryDc) { write(()->doPrimaryDcChanged(currentDc, clusterId, shardId, newPrimaryDc)); }
	void doPrimaryDcChanged(String currentDc, String clusterId, String shardId, String newPrimaryDc);
}
