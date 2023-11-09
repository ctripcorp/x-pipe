package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.*;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public class DefaultKeeperMonitor extends AbstractStartStoppable implements KeeperMonitor{

	private KeeperStats keeperStats;

	private MasterStats masterStats = new DefaultMasterStats();
	
	private ReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();
	
	private RedisKeeperServer redisKeeperServer;
	
	public DefaultKeeperMonitor(RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled) {
		this.redisKeeperServer = redisKeeperServer;
		this.keeperStats = new DefaultKeeperStats(redisKeeperServer.getReplId().toString(), scheduled);
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

	@Override
	public MasterStats getMasterStats() {
		return masterStats;
	}

	@Override
	protected void doStart() throws Exception {
		this.keeperStats.start();
	}

	@Override
	protected void doStop() throws Exception {
		this.keeperStats.stop();
	}
}
