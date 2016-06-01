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
	int getZkConnectionTimeoutMillis();

	/**
	 * @return
	 */
	String getZkConnectionString();

	/**
	 * @return
	 */
	int getZkCloseWaitMillis();

	/**
	 * @return
	 */
	String getZkNamespace();

	/**
	 * @return
	 */
	int getZkRetries();

	/**
	 * @return
	 */
	int getSleepMsBetweenRetries();

	/**
	 * @return
	 */
	int getZkSessionTimeoutMillis();

	/**
	 * @return
	 */
   String getZkLeaderLatchRootPath();

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

}
