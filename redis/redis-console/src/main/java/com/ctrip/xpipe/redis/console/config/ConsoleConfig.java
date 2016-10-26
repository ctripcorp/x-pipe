package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

/**
 * @author shyin
 *
 * Oct 19, 2016
 */
public interface ConsoleConfig extends CoreConfig {
	
	String getDatasource();
	
	int getConsoleNotifyRetryTimes();
	
	int getConsoleNotifyRetryInterval();
	
	String getMetaservers();
	
	int getConsoleNotifyThreads();
	
}
