package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

import java.util.Set;

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

	String getHickwallHostPort();

	int getHickwallQueueSize();

	int getRedisReplicationHealthCheckInterval();
}
