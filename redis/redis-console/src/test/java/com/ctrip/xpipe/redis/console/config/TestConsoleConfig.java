package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;

/**
 * @author shyin
 *
 * Oct 28, 2016
 */
public class TestConsoleConfig extends AbstractCoreConfig implements ConsoleConfig {

	@Override
	public String getDatasource() {
		return "fxxpipe";
	}

	@Override
	public int getConsoleNotifyRetryTimes() {
		return 10;
	}

	@Override
	public int getConsoleNotifyRetryInterval() {
		return 100;
	}

	@Override
	public String getMetaservers() {
		return "{\"jq\":\"http://localhost:8080\",\"oy\":\"http://localhost:8080\"}";
	}
	
	@Override
	public int getConsoleNotifyThreads() {
		return 20;
	}

}
