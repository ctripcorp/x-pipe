package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.*;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class NoneKeepersMonitorManager extends AbstractKeepersMonitorManager implements KeepersMonitorManager{
	
	public static class NoneKeeperMonitor implements KeeperMonitor{
		
		private KeeperStats keeperStats;
		private ReplicationStoreStats replicationStoreStats = new DefaultReplicationStoreStats();

		public NoneKeeperMonitor(ScheduledExecutorService scheduled) {
			keeperStats = new DefaultKeeperStats(scheduled);
		}

		@Override
		public CommandStoreDelay createCommandStoreDelay(CommandStore commandStore) {
			return new NoneCommandStoreDelay();
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

	@Override
	protected KeeperMonitor createKeeperMonitor(RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled) {
		return new NoneKeeperMonitor(scheduled);
	}
}
