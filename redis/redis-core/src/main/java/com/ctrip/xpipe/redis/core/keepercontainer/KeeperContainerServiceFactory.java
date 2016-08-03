package com.ctrip.xpipe.redis.core.keepercontainer;


import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;

/**
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface KeeperContainerServiceFactory {

	KeeperContainerService getOrCreateKeeperContainerService(KeeperContainerMeta keeperContainerMeta);
}
