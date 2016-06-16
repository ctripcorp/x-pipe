package com.ctrip.xpipe.redis.keeper.config;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:06:45 AM
 */
public interface KeeperConfig {

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

}
