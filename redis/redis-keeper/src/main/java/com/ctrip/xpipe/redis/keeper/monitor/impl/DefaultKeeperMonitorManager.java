package com.ctrip.xpipe.redis.keeper.monitor.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitorManager;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class DefaultKeeperMonitorManager implements KeeperMonitorManager{
	
	@Autowired
	public KeeperConfig keeperConfig;
	
	@Override
	public CommandStoreDelay createCommandStoreDelay(CommandStore commandStore) {
		return new DefaultCommandStoreDelay(commandStore, keeperConfig.getDelayLogLimitMicro());
	}
	
	public void setKeeperConfig(KeeperConfig keeperConfig) {
		this.keeperConfig = keeperConfig;
	}
}
