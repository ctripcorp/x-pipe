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
	String KEY_BLOCK_SIZE_THRESHOLD = "keeper.block.size.threshold";
	String KEY_ASYNC_WRITE_MAX_BYTES = "keeper.async.write.max.bytes";
	String KEY_ASYNC_FSYNC_INTERVAL_BYTES = "keeper.async.fsync.interval.bytes";
	String KEY_ASYNC_FSYNC_INTERVAL_MILLIS = "keeper.async.fsync.interval.millis";
	String KEY_ASYNC_IO_THREADS = "keeper.async.io.threads";
	/**
	 * TailCache global memory cap (Direct ByteBuf). When unset, defaults to
	 * {@link #DEFAULT_ASYNC_TAIL_CACHE_MAX_SIZE_RATIO} of JVM MaxDirectMemory
	 * (see startup.sh: MaxDirectMemorySize = 45% machine mem, cap 16G).
	 */
	String KEY_ASYNC_TAIL_CACHE_MAX_SIZE_BYTES = "keeper.async.tail.cache.max.size.bytes";

	long DEFAULT_ASYNC_FSYNC_INTERVAL_BYTES = 1024L * 1024L;
	long DEFAULT_ASYNC_FSYNC_INTERVAL_MILLIS = 1000L;
	int DEFAULT_ASYNC_IO_THREADS = Runtime.getRuntime().availableProcessors() * 8;
	double DEFAULT_ASYNC_TAIL_CACHE_MAX_SIZE_RATIO = 0.8d;
	long DEFAULT_ASYNC_TAIL_CACHE_MAX_SIZE_BYTES_FLOOR = 256L * 1024 * 1024;

	String KEY_ASYNC_TAIL_CACHE_MAX_SIZE_PER_FILE_BYTES = "keeper.async.tail.cache.max.size.per.file.bytes";
	long DEFAULT_ASYNC_TAIL_CACHE_MAX_SIZE_PER_FILE_BYTES = 100L * 1024 * 1024;
	String KEY_UNCLOSED_HANDLE_FLUSH_RETRY_WINDOW_MILLI = "keeper.unclosed.handle.flush.retry.window.milli";
	long DEFAULT_UNCLOSED_HANDLE_FLUSH_RETRY_WINDOW_MILLI = 5L * 60 * 1000;
	String KEY_META_FLUSH_INTERVAL_MILLI = "keeper.meta.flush.interval.milli";
	long DEFAULT_META_FLUSH_INTERVAL_MILLI = 3L * 60 * 1000;
	// keeper-tfs-m6 §17.3: debug file channel fault injection.
	String KEY_FS_DEBUG_ENABLED = "keeper.fs.debug.enabled";
	String KEY_FS_DEBUG_FAULT_EIO = "keeper.fs.debug.fault.eio";
	String KEY_FS_DEBUG_FAULT_DELAY_MILLI = "keeper.fs.debug.fault.delay.milli";
	String KEY_FS_DEBUG_FAULT_OPS = "keeper.fs.debug.fault.ops";
	String KEY_FS_DEBUG_FAULT_PATH_CONTAINS = "keeper.fs.debug.fault.path.contains";
	int DEFAULT_PREPARE_WATCH_REOPEN_INTERVAL_MILLI = 5000;
	int DEFAULT_PREPARE_WATCH_CLOSE_HOLD_MILLI = 1000;
	int DEFAULT_COMMAND_OFFSET_NOTIFY_BYTES_THRESHOLD = 4 * 1024;
	int DEFAULT_COMMAND_OFFSET_NOTIFY_TIME_MILLI_THRESHOLD = 1;


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

	int getCommandOffsetNotifyBytesThreshold();

	int getCommandOffsetNotifyTimeMilliThreshold();

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

    int getCrossRegionFsyncGraceSeconds();

    int getCrossRegionFsyncSettleSeconds();

    boolean fsyncRateLimit();

	boolean tryRorRdb();
	int getXsyncMaxGap();

	int getXsyncMaxGapCrossRegion();

    int getApplierNettyRecvBufferSize();

	boolean getRecordWrongStream();

	boolean isPrepareStoreWatchEnabled();

	/**
	 * 只读句柄保持打开的最小时长，到点进关相（m5 D48）。
	 */
	int getPrepareWatchReopenIntervalMilli();

	/**
	 * 只读句柄关相的最小静置时长，到点才允许 open（m5 D48 / FS-M5.5）。
	 */
	int getPrepareWatchCloseHoldMilli();

	boolean isPubsubParseEnabled();

	boolean stopWriteCk();

	int getRedisMaxBytesLimit();

	int getRedisMinBytesLimit();

	int getRedisRateCheckInterval();

	int getRedisRateIncreaseCheckRounds();

	int getRedisRateDecreaseCheckRounds();

	boolean isRedisRateLimitEnabled();

	boolean dualWrite();

	boolean readV2();

	int getIndexZoneConsecutiveThreshold();

	long getIndexMixedTotalBytesThreshold();

	int getBlockSizeThreshold();

	int getAsyncWriteMaxBytes();

	long getAsyncFsyncIntervalBytes();

	long getAsyncFsyncIntervalMillis();

	int getAsyncIoThreads();

	/**
	 * Max bytes for TailCacheFileSystem local cache (off-heap Direct buffers).
	 */
	long getAsyncTailCacheMaxSizeBytes();

	/** keeper-tfs-m6 §7: TailCache cap for a single file; read at FS creation only. */
	long getAsyncTailCacheMaxSizePerFileBytes();

	/**
	 * keeper-tfs-m6 §2.3: how long the deferred closer keeps flushing a write handle whose close
	 * failed before it degrades to noFlush. Read when a handle is deferred.
	 */
	long getUnclosedHandleFlushRetryWindowMilli();
	/** keeper-tfs-m6 §11.2: periodic meta flush interval; read on every check, ≤ 0 disables. */
	long getMetaFlushIntervalMilli();

	/** keeper-tfs-m6 §17.3: use the debug file channel; read at FS creation only. Default false. */
	boolean isFsDebugEnabled();

	/** keeper-tfs-m6 §17.3: injected faults, reloaded by the injector every second. */
	boolean isFsDebugFaultEio();

	long getFsDebugFaultDelayMilli();

	/** Comma separated OPEN / READ / WRITE / FORCE, "*" for all; default empty injects nothing. */
	String getFsDebugFaultOps();

	String getFsDebugFaultPathContains();
}
