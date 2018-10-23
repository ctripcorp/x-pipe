package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public class DefaultKeeperMonitor implements KeeperMonitor{

	private KeeperStats keeperStats;
	
	private ReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();
	
	private RedisKeeperServer redisKeeperServer;
	
	public DefaultKeeperMonitor(RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled) {
		this.redisKeeperServer = redisKeeperServer;
		this.keeperStats = new DefaultKeeperStats(scheduled);
	}
	
	@Override
	public CommandStoreDelay createCommandStoreDelay(CommandStore commandStore) {
		return new DefaultCommandStoreDelay(commandStore, () -> redisKeeperServer.getKeeperConfig().getDelayLogLimitMicro());
	}

	@Override
	public KeeperStats getKeeperStats() {
		return keeperStats;
	}

	@Override
	public ReplicationStoreStats getReplicationStoreStats() {
		return replicationStoreStats;
	}

}
