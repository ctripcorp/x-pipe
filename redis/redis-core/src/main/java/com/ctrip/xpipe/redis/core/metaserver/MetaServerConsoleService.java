package com.ctrip.xpipe.redis.core.metaserver;

import java.util.List;

import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleService extends MetaServerService{

	void addKeeper(KeeperTransMeta keeperTransMeta);

	void removeKeeper(KeeperTransMeta keeperTransMeta);

	void setKeepers(List<KeeperTransMeta> keeperTransMeta);

	/**
	 * add/delete/modify
	 * meta server found the change, and change the meta
	 * @param clusterId
	 */
	void clusterChanged(String clusterId);
}
