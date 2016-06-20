/**
 * 
 */
package com.ctrip.xpipe.redis.core;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:07:43 PM
 */
public interface CoreConfig {

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
	
	String getZkLeaderLatchRootPath();
}
