package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.CoreConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:06:45 AM
 */
public interface KeeperConfig extends CoreConfig{

	long DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS = 5000L;

	String KEY_LEAKY_BUCKET_INIT_SIZE = "leaky.bucket.init.size";

	String KEY_STOP_WRITE_CK = "keeper.stop.write.ck";
	String KEY_CMD_BATCH_WRITE_SIZE = "keeper.cmd.batch.write.size";
	String KEY_CMD_BATCH_FLUSH_INTERVAL_MILLIS = "keeper.cmd.batch.flush.interva.millis";
	String KEY_CMD_BATCH_LOW_RATE_BPS = "keeper.cmd.batch.low.rate.bps";
	String KEY_BLOCK_SIZE_THRESHOLD = "keeper.block.size.threshold";
	String KEY_ASYNC_WRITE_MAX_BYTES = "keeper.async.write.max.bytes";
	String KEY_ASYNC_FSYNC_INTERVAL_BYTES = "keeper.async.fsync.interval.bytes";
	String KEY_ASYNC_IO_THREADS = "keeper.async.io.threads";

	long DEFAULT_ASYNC_FSYNC_INTERVAL_BYTES = 1024L * 1024L;
	int DEFAULT_ASYNC_IO_THREADS = Runtime.getRuntime().availableProcessors() * 8;


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

	boolean isCommandOffsetNotifyCoalescingEnabled();

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

	int getRedisRateIncreaseCheckRounds();

	int getRedisRateDecreaseCheckRounds();

	boolean isRedisRateLimitEnabled();

	int getCmdBatchWriteSize();

	long getCmdBatchFlushIntervalMillis();

	int getCmdBatchLowRateBps();

	boolean dualWrite();

	boolean readV2();

	int getIndexZoneConsecutiveThreshold();

	long getIndexMixedTotalBytesThreshold();

	int getBlockSizeThreshold();

	int getAsyncWriteMaxBytes();

	long getAsyncFsyncIntervalBytes();

	int getAsyncIoThreads();
}
