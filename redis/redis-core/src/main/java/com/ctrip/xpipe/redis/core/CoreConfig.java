package com.ctrip.xpipe.redis.core;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:07:43 PM
 */
public interface CoreConfig {

	String getZkConnectionString();

	
	String getZkLeaderLatchRootPath();
}
