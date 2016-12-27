package com.ctrip.xpipe.redis.meta.server.meta;

import java.util.List;
import java.util.Set;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;

/**
 * @author wenchao.meng
 *
 *         Aug 7, 2016
 */
public interface DcMetaCache extends Observable {

	Set<String> getClusters();

	String getCurrentDc();

	ClusterMeta getClusterMeta(String clusterId);

	KeeperContainerMeta getKeeperContainer(KeeperMeta keeperMeta);

	boolean isCurrentDcPrimary(String clusterId, String shardId);

	boolean isCurrentDcPrimary(String clusterId);

	Set<String> getBakupDcs(String clusterId, String shardId);

	String getPrimaryDc(String clusterId, String shardId);

	List<KeeperMeta> getShardKeepers(String clusterId, String shardId);

	List<RedisMeta> getShardRedises(String clusterId, String shardId);

	SentinelMeta getSentinel(String clusterId, String shardId);

	String getSentinelMonitorName(String clusterId, String shardId);
	
	
	void clusterAdded(ClusterMeta clusterMeta);

	void clusterModified(ClusterMeta clusterMeta);

	void clusterDeleted(String clusterId);
	
	void primaryDcChanged(String clusterId, String shardId, String newPrimaryDc);
}
