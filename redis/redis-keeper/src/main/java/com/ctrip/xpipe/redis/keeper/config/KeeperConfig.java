package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

import java.util.List;
import java.util.Map;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:06:45 AM
 */
public interface KeeperConfig extends CoreConfig{

	long DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS = 5000L;

	String KEY_LEAKY_BUCKET_INIT_SIZE = "leaky.bucket.init.size";

	String KEY_STOP_WRITE_CK = "keeper.stop.write.ck";

	int getMetaServerConnectTimeout();

	int getMetaServerReadTimeout();

	int getMetaRefreshIntervalMillis();

	String getMetaServerAddress();

	int getReplicationStoreCommandFileSize();

	int getReplicationStoreGcIntervalSeconds();

	int getReplicationStoreCommandFileKeepTimeSeconds();

	int getReplicationStoreCommandFileNumToKeep();

	int getReplicationStoreCommandFileRetainTimeoutMilli();


	/**
	 * max commands transfered before create new rdb
	 * @return
	 */
	long getReplicationStoreMaxCommandsToTransferBeforeCreateRdb();

	long getReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb();

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

	int getApplierReadIdleSeconds();

	int getKeeperIdleSeconds();

    int getKeyReplicationTimeoutMilli();

	/**
	 * max redis slaves allowed to loading rdb at the same time
	 * avoid cross region slaves loading RDB at the same time
	 * -1 means no limit
	 */
    int getCrossRegionMaxLoadingSlavesCnt();

    boolean fsyncRateLimit();

	boolean tryRorRdb();
	int getXsyncMaxGap();

	int getXsyncMaxGapCrossRegion();

    int getApplierNettyRecvBufferSize();

	boolean getRecordWrongStream();

	boolean stopWriteCk();

	int getRedisMaxBytesLimit();

	int getRedisMinBytesLimit();

	int getRedisRateCheckInterval();

}
