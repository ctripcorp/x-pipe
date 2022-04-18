package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcCheckMessage;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;

/**
 * @author marsqing
 *
 *         May 25, 2016 2:37:05 PM
 */
public interface MetaServer extends ClusterServer, TopElement{

	PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc, ForwardInfo forwardInfo);
	
	MetaServerConsoleService.PreviousPrimaryDcMessage makeMasterReadOnly(String clusterId, String shardId, boolean readOnly, ForwardInfo forwardInfo);
	
	PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MetaServerConsoleService.PrimaryDcChangeRequest request, ForwardInfo forwardInfo);

	KeeperMeta getActiveKeeper(String clusterId, String shardId, ForwardInfo forwardInfo);

	RedisMeta getCurrentCRDTMaster(String clusterId, String shardId, ForwardInfo forwardInfo);

	String getSids(String clusterId, String shardId, ForwardInfo forwardInfo);

	RedisMeta getCurrentMaster(String clusterId, String shardId, ForwardInfo forwardInfo);

	RedisMeta getRedisMaster(String clusterId, String shardId);

	void updateUpstream(String clusterId, String shardId, String ip, int port, String sid, ForwardInfo forwardInfo);

	void upstreamPeerChange(String upstreamDcId, String clusterId, String shardId, ForwardInfo forwardInfo);

	void clusterAdded(ClusterMeta clusterMeta, ForwardInfo forwardInfo);

	void clusterModified(ClusterMeta clusterMeta, ForwardInfo forwardInfo);

	void clusterDeleted(String clusterId, ForwardInfo forwardInfo);

	String getCurrentMeta();

}
