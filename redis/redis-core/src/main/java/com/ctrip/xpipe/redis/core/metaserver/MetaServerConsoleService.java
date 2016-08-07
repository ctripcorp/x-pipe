package com.ctrip.xpipe.redis.core.metaserver;



import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleService extends MetaServerService{

	void shardChanged(String clusterId, ShardMeta shardMeta);

	void clusterChanged(ClusterMeta clusterMeta);

	/**
	 * used by backup dc
	 * @param clusterId
	 * @param shardId
	 * @param upstreamAddress
	 */
	void upstreamChange(String clusterId, String shardId, InetSocketAddress upstreamAddress);
	/**
	 * @return
	 */
	DcMeta getDynamicInfo();
}
