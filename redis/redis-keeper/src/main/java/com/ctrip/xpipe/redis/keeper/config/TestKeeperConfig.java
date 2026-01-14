package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultCommandStore;

import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.DEFAULT_XSYNC_MAXGAP;
import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.DEFAULT_XSYNC_MAXGAP_CROSSREGION;

/**
 * @author wenchao.meng
 *
 * Aug 18, 2016
 */
public class TestKeeperConfig extends AbstractCoreConfig implements KeeperConfig{

	private int replicationStoreGcIntervalSeconds = 2;
	private int replicationStoreCommandFileSize = 1024;
	private int replicationStoreCommandFileNumToKeep = 2;
	private long replicationStoreMaxCommandsToTransferBeforeCreateRdb = 1024;
	private long replicationStoreMaxLWMDistanceToTransferBeforeCreateRdb = 10000;
	private int minTimeMilliToGcAfterCreate = 2000;
	public int rdbDumpMinIntervalMilli = 1000;
	private int maxPartialSyncKeepTokenRounds = 3;
	private int partialSyncTrafficMonitorIntervalTimes = 10;
	private long commandReaderFlyingThreshold = DefaultCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD;
	private int maxAllowedMaxGap = DEFAULT_XSYNC_MAXGAP;
	private int maxMaxGapCrossRegion = DEFAULT_XSYNC_MAXGAP_CROSSREGION;
	
	private String zkAddress = System.getProperty("zkAddress", "localhost:2181");
	
	
	public TestKeeperConfig(){
		
	}

	public TestKeeperConfig(int replicationStoreCommandFileSize, int replicationStoreCommandFileNumToKeep, 
			long replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate) {
		this.replicationStoreCommandFileNumToKeep = replicationStoreCommandFileNumToKeep;
		this.replicationStoreCommandFileSize = replicationStoreCommandFileSize;
		this.replicationStoreMaxCommandsToTransferBeforeCreateRdb = replicationStoreMaxCommandsToTransferBeforeCreateRdb;
		this.minTimeMilliToGcAfterCreate = minTimeMilliToGcAfterCreate;
	}

	public TestKeeperConfig(int replicationStoreCommandFileSize, int replicationStoreCommandFileNumToKeep,
							long replicationStoreMaxCommandsToTransferBeforeCreateRdb, int minTimeMilliToGcAfterCreate,
							int keyReplicationTimeoutMilli) {
		this.replicationStoreCommandFileNumToKeep = replicationStoreCommandFileNumToKeep;
		this.replicationStoreCommandFileSize = replicationStoreCommandFileSize;
		this.replicationStoreMaxCommandsToTransferBeforeCreateRdb = replicationStoreMaxCommandsToTransferBeforeCreateRdb;
		this.minTimeMilliToGcAfterCreate = minTimeMilliToGcAfterCreate;
		this.keyReplicationTimeoutMilli = keyReplicationTimeoutMilli;
	}

	@Override
	public int getMetaServerConnectTimeout() {
		return 1000;
	}

	@Override
	public int getMetaServerReadTimeout() {
		return 1000;
	}

	@Override
	public int getMetaRefreshIntervalMillis() {
		return 60000;
	}

	@Override
	public String getMetaServerAddress() {
		return "";
	}

	@Override
	public int getReplicationStoreCommandFileSize() {
		return replicationStoreCommandFileSize;
	}

	@Override
	public int getReplicationStoreGcIntervalSeconds() {
		return replicationStoreGcIntervalSeconds;
	}

	public void setReplicationStoreGcIntervalSeconds(int replicationStoreGcIntervalSeconds) {
		this.replicationStoreGcIntervalSeconds = replicationStoreGcIntervalSeconds;
	}
	
	@Override
	public int getReplicationStoreCommandFileNumToKeep() {
		return replicationStoreCommandFileNumToKeep;
	}

	@Override
	public int getReplicationStoreCommandFileRetainTimeoutMilli() {
		return 2 * 3600 * 1000;
	}

	@Override
	public long getReplicationStoreMaxCommandsToTransferBeforeCreateRdb() {
		return replicationStoreMaxCommandsToTransferBeforeCreateRdb;
	}

	@Override
	public long getReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb() {
		return replicationStoreMaxLWMDistanceToTransferBeforeCreateRdb;
	}

	public void setReplicationStoreCommandFileNumToKeep(int replicationStoreCommandFileNumToKeep) {
		this.replicationStoreCommandFileNumToKeep = replicationStoreCommandFileNumToKeep;
	}
	
	public void setReplicationStoreCommandFileSize(int replicationStoreCommandFileSize) {
		this.replicationStoreCommandFileSize = replicationStoreCommandFileSize;
	}

	public void setReplicationStoreMaxCommandsToTransferBeforeCreateRdb(
			int replicationStoreMaxCommandsToTransferBeforeCreateRdb) {
		this.replicationStoreMaxCommandsToTransferBeforeCreateRdb = replicationStoreMaxCommandsToTransferBeforeCreateRdb;
	}

	public void setReplicationStoreMaxLWMDistanceToTransferBeforeCreateRdb(
			int replicationStoreMaxLWMDistanceToTransferBeforeCreateRdb) {
		this.replicationStoreMaxLWMDistanceToTransferBeforeCreateRdb = replicationStoreMaxLWMDistanceToTransferBeforeCreateRdb;
	}

	@Override
	public int getRdbDumpMinIntervalMilli() {
		return rdbDumpMinIntervalMilli;
	}
	
	@Override
	public String getZkConnectionString() {
		return zkAddress;
	}

	public void setZkAddress(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	@Override
	public int getReplicationStoreMinTimeMilliToGcAfterCreate() {
		return minTimeMilliToGcAfterCreate;
	}

	public void setCommandReaderFlyingThreshold(long commandReaderFlyingThreshold) {
		this.commandReaderFlyingThreshold = commandReaderFlyingThreshold;
	}

	@Override
	public long getCommandReaderFlyingThreshold() {
		return commandReaderFlyingThreshold;
	}

	@Override
	public int getCommandIndexBytesInterval() {
		return 256;
	}

	public void setMinTimeMilliToGcAfterCreate(int minTimeMilliToGcAfterCreate) {
		this.minTimeMilliToGcAfterCreate = minTimeMilliToGcAfterCreate;
	}
	
	public void setRdbDumpMinIntervalMilli(int rdbDumpMinIntervalMilli) {
		this.rdbDumpMinIntervalMilli = rdbDumpMinIntervalMilli;
	}

	@Override
	public int getDelayLogLimitMicro() {
		return 20*1000;
	}

	@Override
    public long getTrafficReportIntervalMillis() {
        return 10000L;
    }

    private long replLowWaterMark = 100L * 1024 * 1024;
	@Override
	public long getReplicationTrafficHighWaterMark() {
		return replHighWaterMark;
	}

	private long replHighWaterMark = 20L * 1024 * 1024;
	@Override
	public long getReplicationTrafficLowWaterMark() {
		return replLowWaterMark;
	}

	public TestKeeperConfig setReplLowWaterMark(long replLowWaterMark) {
		this.replLowWaterMark = replLowWaterMark;
		return this;
	}

	public TestKeeperConfig setReplHighWaterMark(long replHighWaterMark) {
		this.replHighWaterMark = replHighWaterMark;
		return this;
	}

	@Override
	public int getLeakyBucketInitSize() {
		return 3;
	}

	public TestKeeperConfig setPartialSyncTrafficMonitorIntervalTimes(int partialSyncTrafficMonitorIntervalTimes) {
		this.partialSyncTrafficMonitorIntervalTimes = partialSyncTrafficMonitorIntervalTimes;
		return this;
	}


	public TestKeeperConfig setMaxPartialSyncKeepTokenRounds(int maxPartialSyncKeepTokenRounds) {
		this.maxPartialSyncKeepTokenRounds = maxPartialSyncKeepTokenRounds;
		return this;
	}

	private boolean keeperRateLimit = true;

	@Override
	public boolean isKeeperRateLimitOpen() {
		return keeperRateLimit;
	}

	private long replDownSafeIntervalMilli = 500;
	@Override
	public long getReplDownSafeIntervalMilli() {
		return replDownSafeIntervalMilli;
	}

	private long replKeepSecondsAfterDown = 60;

	public void setReplKeepSecondsAfterDown(long replKeepSecondsAfterDown) {
		this.replKeepSecondsAfterDown = replKeepSecondsAfterDown;
	}

	@Override
	public long getMaxReplKeepSecondsAfterDown() {
		return replKeepSecondsAfterDown;
	}

	public void setKeeperRateLimit(boolean keeperRateLimit) {
		this.keeperRateLimit = keeperRateLimit;
	}

	public TestKeeperConfig setReplDownSafeIntervalMilli(long replDownSafeIntervalMilli) {
		this.replDownSafeIntervalMilli = replDownSafeIntervalMilli;
		return this;
	}

	private int cmdFileKeepSeconds = 60;

	public void setReplicationStoreCommandFileKeepTimeSeconds(int seconds) {
		this.cmdFileKeepSeconds = seconds;
	}

	@Override
	public int getReplicationStoreCommandFileKeepTimeSeconds() {
		return cmdFileKeepSeconds;
	}

	private int keyReplicationTimeoutMilli = 60000;

	@Override
	public int getKeyReplicationTimeoutMilli() {
	    return keyReplicationTimeoutMilli;
	}

	private int maxLoadingSlaves = 1;

	public TestKeeperConfig setMaxLoadingSlaves(int slaves) {
		this.maxLoadingSlaves = slaves;
		return this;
	}

	@Override
	public int getCrossRegionMaxLoadingSlavesCnt() {
		return maxLoadingSlaves;
	}

	@Override
	public int getApplierReadIdleSeconds() {
		return 60;
	}

	@Override
	public int getKeeperIdleSeconds() {
		return 0;
	}

	@Override
	public boolean fsyncRateLimit() {
		return true;
	}

	@Override
	public boolean tryRorRdb() {
		return true;
	}

	@Override
	public int getXsyncMaxGap() {
		return maxAllowedMaxGap;
	}

	@Override
	public int getXsyncMaxGapCrossRegion() {
		return maxMaxGapCrossRegion;
	}

	public void setXsyncMaxGap(int maxGap) {
		this.maxAllowedMaxGap = maxGap;
	}

	public void setXsyncMaxGapCrossRegion(int maxGapCrossRegion) {
		this.maxMaxGapCrossRegion = maxGapCrossRegion;
	}

    @Override
    public int getApplierNettyRecvBufferSize() {
        return 512;
    }

	@Override
	public boolean getRecordWrongStream() {
		return false;
	}

	@Override
	public boolean stopWriteCk() {
		return false;
	}
}
