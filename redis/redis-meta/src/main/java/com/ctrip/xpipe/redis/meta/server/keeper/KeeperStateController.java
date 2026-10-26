package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface KeeperStateController {

	void addKeeper(KeeperTransMeta keeperTransMeta);

	/**
	 * @param purge true when the shard/cluster this keeper belonged to is gone (or has no keepers
	 *              left), so the store should be deleted; false keeps a shared TFS store alive
	 *              (migration, dead-keeper cleanup) — see xpipe-keeper-destroy-purge-design.
	 */
	void removeKeeper(KeeperTransMeta keeperTransMeta, boolean purge);

}
