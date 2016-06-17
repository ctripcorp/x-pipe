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
