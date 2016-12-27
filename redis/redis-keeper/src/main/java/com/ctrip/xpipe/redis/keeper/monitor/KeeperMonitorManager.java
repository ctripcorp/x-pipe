package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.store.CommandStore;

/**
 * @author wenchao.meng
 *
 * Nov 24, 2016
 */
public interface KeeperMonitorManager {
	
	CommandStoreDelay createCommandStoreDelay(CommandStore commandStore);
	
}
