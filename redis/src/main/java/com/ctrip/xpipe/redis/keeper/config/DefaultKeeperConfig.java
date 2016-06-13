package com.ctrip.xpipe.redis.keeper.config;

import org.springframework.stereotype.Component;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:12:35 AM
 */
@Component
public class DefaultKeeperConfig implements KeeperConfig {

	@Override
	public int getZkConnectionTimeoutMillis() {
		return 3000;
	}

	@Override
	public String getZkConnectionString() {
		// TODO
		return "127.0.0.1:2181";
	}

	@Override
	public int getZkCloseWaitMillis() {
		return 1000;
	}

	@Override
	public String getZkNamespace() {
		return "xpipe";
	}

	@Override
	public int getZkRetries() {
		return Integer.MAX_VALUE;
	}

	@Override
	public int getSleepMsBetweenRetries() {
		return 1000;
	}

	@Override
	public int getZkSessionTimeoutMillis() {
		return 5000;
	}

	@Override
	public String getZkLeaderLatchRootPath() {
		return "/keepers";
	}

	@Override
	public int getMetaServerConnectTimeout() {
		return 2000;
	}

	@Override
	public int getMetaServerReadTimeout() {
		return 2000;
	}

	@Override
	public int getRedisCommandFileSize() {
		return 1024 * 1024 * 1024;
	}

	@Override
	public int getMetaRefreshIntervalMillis() {
		return 3000;
	}

}
