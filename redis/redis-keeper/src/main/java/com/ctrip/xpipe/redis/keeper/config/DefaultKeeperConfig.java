package com.ctrip.xpipe.redis.keeper.config;

import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:12:35 AM
 */
@Component
public class DefaultKeeperConfig extends AbstractCoreConfig implements KeeperConfig {

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
		return 20;
	}

	@Override
	public int getMetaRefreshIntervalMillis() {
		return 3000;
	}

	@Override
	public long getReplicationStoreGcIntervalSeconds() {
		return 10;
	}

}
