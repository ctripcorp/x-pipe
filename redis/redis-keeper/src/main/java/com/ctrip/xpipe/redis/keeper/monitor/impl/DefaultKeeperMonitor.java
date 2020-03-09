package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public class DefaultKeeperMonitor extends AbstractStartStoppable implements KeeperMonitor{

	private KeeperStats keeperStats;
	
	private ReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();
	
	private RedisKeeperServer redisKeeperServer;
	
	public DefaultKeeperMonitor(RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled) {
		this.redisKeeperServer = redisKeeperServer;
		this.keeperStats = new DefaultKeeperStats(redisKeeperServer.getShardId(), scheduled);
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
	protected void doStart() throws Exception {
		this.keeperStats.start();
	}

	@Override
	protected void doStop() throws Exception {
		this.keeperStats.stop();
	}
}
