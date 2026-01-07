package com.ctrip.xpipe.redis.keeper.config;


import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication;

import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.DEFAULT_XSYNC_MAXGAP;
import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.DEFAULT_XSYNC_MAXGAP_CROSSREGION;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:12:35 AM
 */
public class DefaultKeeperConfig extends AbstractCoreConfig implements KeeperConfig {
	
	public static final String KEY_REPLICATION_STORE_GC_INTERVAL_SECONDS = "replicationstore.gc.interval.seconds";
	public static final String KEY_REPLICATION_STORE_CMD_KEEP_TIME_SECONDS = "replicationstore.commandfile.keeptime.seconds";
	public static final String KEY_REPLICATION_STORE_COMMANDFILE_SIZE = "replicationstore.commandfile.size";
	public static final String KEY_REPLICATION_STORE_COMMANDFILE_NUM_KEEP = "replicationstore.commandfile.num.keep";
	public static final String KEY_REPLICATION_STORE_MINITIME_GC_AFTERCREATE = "replicationstore.mintime.gc.aftercreate";
	public static final String KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB = "replicationstore.max.commands.to.transfer";
	public static final String KEY_REPLICATION_STORE_MAX_LWM_DISTANCE_TO_TRANSFER_BEFORE_CREATE_RDB = "replicationstore.max.lwm.distance.to.transfer";
	public static final String KEY_REPLICATION_STORE_COMMANDFILE_RETAIN_TIMEOUT_MILLI = "replicationstore.commandfile.retain.timeout.milli";
	public static final String KEY_COMMAND_READER_FLYING_THRESHOLD = "command.reader.flying.threshold";
	private static final String KEY_COMMAND_INDEX_BYTES_INTERVAL = "command.index.bytes.interval";
	public static final String KEY_RDB_DUMP_MIN_INTERVAL = "rdbdump.min.interval";
	public static final String KEY_DELAY_LOG_LIMIT_MICRO = "monitor.delay.log.limit.micro";
    private static final String KEY_TRAFFIC_REPORT_INTERVAL = "monitor.traffic.report.interval";
	private static final String KEY_KEEPER_RATE_LIMIT_OPEN = "keeper.rate.limit.open";

	private static String KEEPER_CONTAINER_PROPERTIES_PATH = String.format("/opt/data/%s", FoundationService.DEFAULT.getAppId());
	private static String KEEPER_CONTAINER_PROPERTIES_FILE = "keeper-container.properties";

	private static String KYE_REPLICATION_TRAFFIC_HIGH_WATER_MARK = "keeper.repl.traffic.high.water.mark";

	private static String KYE_REPLICATION_TRAFFIC_LOW_WATER_MARK = "keeper.repl.traffic.low.water.mark";

	private static String KYE_REPLICATION_DOWN_SAFE_INTERVAL_MILLI = "keeper.repl.down.safe.interval.milli";

	private static String KYE_REPLICATION_KEEP_SECONDS_AFTER_DOWN = "keeper.repl.keep.seconds.after.down";

	private static String KEY_REPLICATION_TIMEOUT_MILLI = "replication.timeout.milli";

	private static String KEY_META_SERVER_ADDRESS = "meta.server.address";

	private static String KEY_APPLIER_READ_IDLE_SECONDS = "applier.read.idle.seconds";

	private static String KEY_KEEPER_IDLE_SECONDS = "keeper.idle.seconds";

	private static String KEY_CROSS_REGION_MAX_FSYNC_SLAVES = "crossregion.replication.loading.slaves.max";

	private static String KEY_FSYNC_RATE_LIMIT = "keeper.repl.fsync.rate.limit";
	private static String KEY_TRY_ROR_RDB = "keeper.try.ror.rdb";
	private static String KEY_XSYNC_MAX_GAP = "keeper.xsync.max.gap";
	private static String KEY_XSYNC_MAX_GAP_CROSSREGION = "keeper.xsync.max.gap.crossregion";

    private static String KEY_APPLIER_NETTY_RECV_BUFFER_SIZE = "applier.netty.recv.buffer.size";

	private static String KEY_RECORD_WRONG_STREAM = "keeper.record.wrong.stream";

	public DefaultKeeperConfig(){

		CompositeConfig compositeConfig = new CompositeConfig();
		compositeConfig.addConfig(Config.DEFAULT);
		try{
			compositeConfig.addConfig(new DefaultFileConfig(KEEPER_CONTAINER_PROPERTIES_PATH, KEEPER_CONTAINER_PROPERTIES_FILE));
		}catch (Exception e){
//			logger.info("[DefaultKeeperConfig]", e);
		}
		compositeConfig.addConfig(new DefaultPropertyConfig());
		setConfig(compositeConfig);
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
	public int getMetaRefreshIntervalMillis() {
		return 3000;
	}

	@Override
	public String getMetaServerAddress() {
		return getProperty(KEY_META_SERVER_ADDRESS, "");
	}

	@Override
	public int getReplicationStoreGcIntervalSeconds() {
		return getIntProperty(KEY_REPLICATION_STORE_GC_INTERVAL_SECONDS, 2);
	}

	@Override
	public int getReplicationStoreCommandFileKeepTimeSeconds() {
		return getIntProperty(KEY_REPLICATION_STORE_CMD_KEEP_TIME_SECONDS, 2 * 86400);
	}

	@Override
	public int getReplicationStoreCommandFileSize() {
		return getIntProperty(KEY_REPLICATION_STORE_COMMANDFILE_SIZE, 20);
	}

	@Override
	public int getReplicationStoreCommandFileNumToKeep() {
		return getIntProperty(KEY_REPLICATION_STORE_COMMANDFILE_NUM_KEEP, 2);
	}

	@Override
	public int getReplicationStoreCommandFileRetainTimeoutMilli() {
		return getIntProperty(KEY_REPLICATION_STORE_COMMANDFILE_RETAIN_TIMEOUT_MILLI,1800 * 1000);
	}

	@Override
	public long getReplicationStoreMaxCommandsToTransferBeforeCreateRdb() {
		return getLongProperty(KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB, 100L);
	}

	@Override
	public long getReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb() {
		return getLongProperty(KEY_REPLICATION_STORE_MAX_LWM_DISTANCE_TO_TRANSFER_BEFORE_CREATE_RDB, 100000L);
	}

	@Override
	public int getRdbDumpMinIntervalMilli() {
		return getIntProperty(KEY_RDB_DUMP_MIN_INTERVAL, 3600000);
	}

	@Override
	public
	int getReplicationStoreMinTimeMilliToGcAfterCreate(){
		return getIntProperty(KEY_REPLICATION_STORE_MINITIME_GC_AFTERCREATE, 60000);
	}

	@Override
	public long getCommandReaderFlyingThreshold() {
		return getLongProperty(KEY_COMMAND_READER_FLYING_THRESHOLD, (long) (1 << 15));
	}

	@Override
	public int getCommandIndexBytesInterval() {
		return getIntProperty(KEY_COMMAND_INDEX_BYTES_INTERVAL, 50 * 1024 * 1024);
	}

	@Override
	public int getDelayLogLimitMicro() {
		return getIntProperty(KEY_DELAY_LOG_LIMIT_MICRO, 10000);
	}

    @Override
    public long getTrafficReportIntervalMillis() {
        return getLongProperty(KEY_TRAFFIC_REPORT_INTERVAL, DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS);
    }

	@Override
	public long getReplicationTrafficHighWaterMark() {
		return getLongProperty(KYE_REPLICATION_TRAFFIC_HIGH_WATER_MARK, 100L * 1024 * 1024);  // 100MB/s
	}

	@Override
	public long getReplicationTrafficLowWaterMark() {
		return getLongProperty(KYE_REPLICATION_TRAFFIC_LOW_WATER_MARK, 20L * 1024 * 1024); // 20MB/s
	}

	@Override
	public int getLeakyBucketInitSize() {
		return getIntProperty(KEY_LEAKY_BUCKET_INIT_SIZE, 8);
	}

	@Override
	public boolean isKeeperRateLimitOpen() {
		return getBooleanProperty(KEY_KEEPER_RATE_LIMIT_OPEN, true);
	}

	@Override
	public long getReplDownSafeIntervalMilli() {
		return getLongProperty(KYE_REPLICATION_DOWN_SAFE_INTERVAL_MILLI, 5L * 60 * 1000); // 5min
	}

	@Override
	public long getMaxReplKeepSecondsAfterDown() {
		return getLongProperty(KYE_REPLICATION_KEEP_SECONDS_AFTER_DOWN, 3600L);
	}

	@Override
	public int getApplierReadIdleSeconds() {
		return getIntProperty(KEY_APPLIER_READ_IDLE_SECONDS, 60);
	}

	@Override
	public int getKeeperIdleSeconds() {
		// Same as credis, for applier keeper, it is recommended to set the timeout to 0.
		return getIntProperty(KEY_KEEPER_IDLE_SECONDS, 900);
	}

	@Override
	public int getKeyReplicationTimeoutMilli() {
	    return getIntProperty(KEY_REPLICATION_TIMEOUT_MILLI, AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI);
	}

	@Override
	public int getCrossRegionMaxLoadingSlavesCnt() {
		return getIntProperty(KEY_CROSS_REGION_MAX_FSYNC_SLAVES, 1);
	}

	@Override
	public boolean fsyncRateLimit() {
		return getBooleanProperty(KEY_FSYNC_RATE_LIMIT, true);
	}

	@Override
	public boolean tryRorRdb() {
		// capa rordb as default
		// if the master support rordb, the slaves will most likely support it too
		return getBooleanProperty(KEY_TRY_ROR_RDB, true);
	}

	@Override
	public int getXsyncMaxGap() {
		return getIntProperty(KEY_XSYNC_MAX_GAP, DEFAULT_XSYNC_MAXGAP);
	}

	@Override
	public int getXsyncMaxGapCrossRegion() {
		return getIntProperty(KEY_XSYNC_MAX_GAP_CROSSREGION, DEFAULT_XSYNC_MAXGAP_CROSSREGION);
	}

    @Override
    public int getApplierNettyRecvBufferSize() {
        return getIntProperty(KEY_APPLIER_NETTY_RECV_BUFFER_SIZE, 512);
    }

	@Override
	public boolean getRecordWrongStream() {
		return getBooleanProperty(KEY_RECORD_WRONG_STREAM, false);
	}
}
