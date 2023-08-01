package com.ctrip.xpipe.redis.keeper.config;


import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.keeper.impl.AbstractRedisMasterReplication;

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

	private static String KEY_MAX_FSYNC_SLAVES = "replication.loading.slaves.max";

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
	public int getKeyReplicationTimeoutMilli() {
	    return getIntProperty(KEY_REPLICATION_TIMEOUT_MILLI, AbstractRedisMasterReplication.DEFAULT_REPLICATION_TIMEOUT_MILLI);
	}

	@Override
	public int getMaxLoadingSlavesCnt() {
		return getIntProperty(KEY_MAX_FSYNC_SLAVES, -1);
	}
}
