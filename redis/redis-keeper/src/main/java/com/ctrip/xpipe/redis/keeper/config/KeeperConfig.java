package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:06:45 AM
 */
public interface KeeperConfig extends CoreConfig{

	/**
	 * @return
	 */
	int getMetaServerConnectTimeout();

	/**
	 * @return
	 */
	int getMetaServerReadTimeout();

	/**
	 * @return
	 */
	int getRedisCommandFileSize();

	/**
	 * @return
	 */
	int getMetaRefreshIntervalMillis();

	long getReplicationStoreGcIntervalSeconds();

}
