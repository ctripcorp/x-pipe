package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitorManager;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class NoneKeeperMonitorManager implements KeeperMonitorManager{

	@Override
	public CommandStoreDelay createCommandStoreDelay(CommandStore commandStore) {
		return new NoneCommandStoreDelay();
	}

}
