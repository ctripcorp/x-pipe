package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.core.store.CommandStore;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public interface KeeperMonitor {

	CommandStoreDelay createCommandStoreDelay(CommandStore commandStore);
	
	KeeperStats getKeeperStats();
	
	ReplicationStoreStats getReplicationStoreStats();
	
}
