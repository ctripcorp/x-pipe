package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public class DefaultKeeperMonitor implements KeeperMonitor{

	private KeeperStats keeperStats = new DefaultKeeperStats();
	
	private RedisKeeperServer redisKeeperServer;
	
	public DefaultKeeperMonitor(RedisKeeperServer redisKeeperServer) {
		this.redisKeeperServer = redisKeeperServer;
	}
	
	@Override
	public CommandStoreDelay createCommandStoreDelay(CommandStore commandStore) {
		return new DefaultCommandStoreDelay(commandStore, redisKeeperServer.getKeeperConfig().getDelayLogLimitMicro());
	}

	@Override
	public KeeperStats getKeeperStats() {
		return keeperStats;
	}

}
