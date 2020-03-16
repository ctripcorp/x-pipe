package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.core.store.CommandStore;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public interface KeeperMonitor extends Startable, Stoppable {

	CommandStoreDelay createCommandStoreDelay(CommandStore commandStore);
	
	KeeperStats getKeeperStats();
	
	ReplicationStoreStats getReplicationStoreStats();
	
}
