package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 13, 2016
 */
public class DefaultKeepersMonitorManager extends AbstractKeepersMonitorManager implements KeepersMonitorManager{
	
	@Autowired
	public KeeperConfig keeperConfig;

	@Override
	protected KeeperMonitor createKeeperMonitor(RedisKeeperServer redisKeeperServer, ScheduledExecutorService scheduled) {
		return new DefaultKeeperMonitor(redisKeeperServer, scheduled);
	}
	
}
