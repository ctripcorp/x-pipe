package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:06:45 AM
 */
public interface KeeperConfig extends CoreConfig{

	public static final long DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS = 5000L;

	int getMetaServerConnectTimeout();

	int getMetaServerReadTimeout();

	int getMetaRefreshIntervalMillis();

	int getReplicationStoreCommandFileSize();

	int getReplicationStoreGcIntervalSeconds();

	int getReplicationStoreCommandFileNumToKeep();

	/**
	 * max commands transfered before create new rdb
	 * @return
	 */
	long getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();

	int getReplicationStoreMinTimeMilliToGcAfterCreate();

	long getCommandReaderFlyingThreshold();

	int getRdbDumpMinIntervalMilli();
	
	int getDelayLogLimitMicro();

    long getTrafficReportIntervalMillis();
	
}
