package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public abstract class AbstractKeepersMonitorManager implements KeepersMonitorManager{

	private static final int DEFAULT_SCHEDULED_CORE_POOL_SIZE = Math.min(OsUtils.getCpuCount(), 4);
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private Map<RedisKeeperServer, KeeperMonitor> keeperMonitors = new ConcurrentHashMap<>();

	private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(DEFAULT_SCHEDULED_CORE_POOL_SIZE ,
			XpipeThreadFactory.create("globalStatsMetricUpdater"));
	
	@Override
	public KeeperMonitor getOrCreate(RedisKeeperServer redisKeeperServer) {
		
		return MapUtils.getOrCreate(keeperMonitors, redisKeeperServer, new ObjectFactory<KeeperMonitor>() {

			@Override
			public KeeperMonitor create() {
				return createKeeperMonitor(redisKeeperServer, scheduled);
			}
		});
	}

	protected abstract KeeperMonitor createKeeperMonitor(RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled);

	@Override
	public void remove(RedisKeeperServer redisKeeperServer) {
		keeperMonitors.remove(redisKeeperServer);
	}

}
