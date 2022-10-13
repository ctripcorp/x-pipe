package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:06:45 AM
 */
public interface KeeperConfig extends CoreConfig{

	public static final long DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS = 5000L;

	public static final String KEY_LEAKY_BUCKET_INIT_SIZE = "leaky.bucket.init.size";

	int getMetaServerConnectTimeout();

	int getMetaServerReadTimeout();

	int getMetaRefreshIntervalMillis();

	String getMetaServerAddress();

	int getReplicationStoreCommandFileSize();

	int getReplicationStoreGcIntervalSeconds();

	int getReplicationStoreCommandFileKeepTimeSeconds();

	int getReplicationStoreCommandFileNumToKeep();

	/**
	 * max commands transfered before create new rdb
	 * @return
	 */
	long getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();

	int getReplicationStoreMinTimeMilliToGcAfterCreate();

	long getCommandReaderFlyingThreshold();

	int getCommandIndexBytesInterval();

	int getRdbDumpMinIntervalMilli();
	
	int getDelayLogLimitMicro();

    long getTrafficReportIntervalMillis();

    long getReplicationTrafficHighWaterMark();

    long getReplicationTrafficLowWaterMark();

    int getLeakyBucketInitSize();

    boolean isKeeperRateLimitOpen();

    long getReplDownSafeIntervalMilli();

    long getMaxReplKeepSecondsAfterDown();

}
