package com.ctrip.xpipe.redis.keeper.config;


import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.config.CompositeConfig;
import com.ctrip.xpipe.config.DefaultFileConfig;
import com.ctrip.xpipe.config.DefaultPropertyConfig;
import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;

/**
 * @author marsqing
 *
 *         May 25, 2016 11:12:35 AM
 */
public class DefaultKeeperConfig extends AbstractCoreConfig implements KeeperConfig {
	
	public static final String KEY_REPLICATION_STORE_GC_INTERVAL_SECONDS = "replicationstore.gc.interval.seconds";
	public static final String KEY_REPLICATION_STORE_COMMANDFILE_SIZE = "replicationstore.commandfile.size";
	public static final String KEY_REPLICATION_STORE_COMMANDFILE_NUM_KEEP = "replicationstore.commandfile.num.keep";
	public static final String KEY_REPLICATION_STORE_MINITIME_GC_AFTERCREATE = "replicationstore.mintime.gc.aftercreate";
	public static final String KEY_REPLICATION_STORE_MAX_COMMANDS_TO_TRANSFER_BEFORE_CREATE_RDB = "replicationstore.max.commands.to.transfer";
	public static final String KEY_COMMAND_READER_FLYING_THRESHOLD = "command.reader.flying.threshold";
	public static final String KEY_RDB_DUMP_MIN_INTERVAL = "rdbdump.min.interval";
	public static final String KEY_DELAY_LOG_LIMIT_MICRO = "monitor.delay.log.limit.micro";
    private static final String KEY_TRAFFIC_REPORT_INTERVAL = "monitor.traffic.report.interval";

	private static String KEEPER_CONTAINER_PROPERTIES_PATH = String.format("/opt/data/%s", FoundationService.DEFAULT.getAppId());
	private static String KEEPER_CONTAINER_PROPERTIES_FILE = "keeper-container.properties";

	public DefaultKeeperConfig(){

		CompositeConfig compositeConfig = new CompositeConfig();
		compositeConfig.addConfig(Config.DEFAULT);
		try{
			compositeConfig.addConfig(new DefaultFileConfig(KEEPER_CONTAINER_PROPERTIES_PATH, KEEPER_CONTAINER_PROPERTIES_FILE));
		}catch (Exception e){
			logger.info("[DefaultKeeperConfig]{}", e);
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
	public int getReplicationStoreGcIntervalSeconds() {
		return getIntProperty(KEY_REPLICATION_STORE_GC_INTERVAL_SECONDS, 2);
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
	public int getDelayLogLimitMicro() {
		return getIntProperty(KEY_DELAY_LOG_LIMIT_MICRO, 10000);
	}

    @Override
    public long getTrafficReportIntervalMillis() {
        return getLongProperty(KEY_TRAFFIC_REPORT_INTERVAL, DEFAULT_TRAFFIC_REPORT_INTERVAL_MILLIS);
    }
}
