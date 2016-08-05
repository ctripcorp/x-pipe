/**
 * 
 */
package com.ctrip.xpipe.redis.meta.server.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:48:44 AM
 */
public interface MetaServerConfig extends CoreConfig{
	
	
	String getConsoleAddress();
	
	int getMetaRefreshMilli();
	
	int getMetaServerId();
	
	String getMetaServerIp();
	
	int getMetaServerPort();
	

	int getClusterServersRefreshMilli();

	int getSlotRefreshMilli();
	
	int getLeaderCheckMilli();
	
}
