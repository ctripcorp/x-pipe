package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;

/**
 * @author wenchao.meng
 *
 * Aug 5, 2016
 */
public interface KeeperStateController {
	
	void addKeeper(KeeperTransMeta keeperTransMeta);
	
	void removeKeeper(KeeperTransMeta keeperTransMeta);

}
