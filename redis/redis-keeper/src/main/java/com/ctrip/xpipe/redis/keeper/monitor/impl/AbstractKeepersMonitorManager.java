package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.utils.MapUtils;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author wenchao.meng
 *
 * Feb 20, 2017
 */
public abstract class AbstractKeepersMonitorManager implements KeepersMonitorManager{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private Map<RedisKeeperServer, KeeperMonitor> keeperMonitors = new ConcurrentHashMap<>();
	
	@Override
	public KeeperMonitor getOrCreate(RedisKeeperServer redisKeeperServer) {
		
		return MapUtils.getOrCreate(keeperMonitors, redisKeeperServer, new ObjectFactory<KeeperMonitor>() {

			@Override
			public KeeperMonitor create() {
				return createKeeperMonitor(redisKeeperServer);
			}
		});
	}

	protected abstract KeeperMonitor createKeeperMonitor(RedisKeeperServer redisKeeperServer);

	@Override
	public void remove(RedisKeeperServer redisKeeperServer) {
		keeperMonitors.remove(redisKeeperServer);
	}

}
