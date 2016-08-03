package com.ctrip.xpipe.redis.core.metaserver;


import java.util.List;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleService extends MetaServerService{

	void addKeeper(String clusterId, String shardId,  KeeperMeta keeperMeta);

	void removeKeeper(String clusterId, String shardId,  KeeperMeta keeperMeta);

	void setKeepers(String clusterId, String shardId,  KeeperMeta keeperMeta, List<KeeperMeta> keeperMetas);

	/**
	 * add/delete/modify
	 * meta server found the change, and change the meta
	 * @param clusterId
	 */
	void clusterChanged(String clusterId);
	
	/**
	 * @return
	 */
	DcMeta getDynamicInfo();
}
