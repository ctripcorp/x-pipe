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
public interface MetaUpdateOperation {

	boolean updateKeeperActive(String dc, String clusterId, String shardId, KeeperMeta activeKeeper);
	
	boolean noneKeeperActive(String currentDc, String clusterId, String shardId);
	
	boolean updateRedisMaster(String dc, String clusterId, String shardId, RedisMeta redisMaster);
	
	void update(DcMeta dcMeta);

	void update(String dcId, ClusterMeta clusterMeta);
	
	ClusterMeta removeCluster(String currentDc, String clusterId);
	
	void setSurviveKeepers(String dcId, String clusterId, String shardId, List<KeeperMeta> surviceKeepers);

}
