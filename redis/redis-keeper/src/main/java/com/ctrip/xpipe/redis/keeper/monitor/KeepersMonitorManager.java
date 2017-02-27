package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

/**
 * @author wenchao.meng
 *
 * Nov 24, 2016
 */
public interface KeepersMonitorManager {
	
	KeeperMonitor getOrCreate(RedisKeeperServer redisKeeperServer);
	
	void remove(RedisKeeperServer redisKeeperServer);
}
