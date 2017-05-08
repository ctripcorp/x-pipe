package com.ctrip.xpipe.redis.console.config;

import java.util.Set;

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

	Set<String> getConsoleUserAccessWhiteList();
	
	int getRedisReplicationHealthCheckInterval();
	
	String getHickwallAddress();

	int getHealthyDelayMilli();

	int getDownAfterCheckNums();

	int getCacheRefreshInterval();

}
