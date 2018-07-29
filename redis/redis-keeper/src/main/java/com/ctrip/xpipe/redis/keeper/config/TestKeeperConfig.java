package com.ctrip.xpipe.redis.keeper.config;

import com.ctrip.xpipe.redis.core.config.AbstractCoreConfig;
import com.ctrip.xpipe.redis.keeper.store.DefaultCommandStore;

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
	private int minTimeMilliToGcAfterCreate = 2000;
	private int rdbDumpMinIntervalMilli = 1000;
	
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
	public long getReplicationStoreMaxCommandsToTransferBeforeCreateRdb() {
		return replicationStoreMaxCommandsToTransferBeforeCreateRdb;
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

	@Override
	public long getCommandReaderFlyingThreshold() {
		return DefaultCommandStore.DEFAULT_COMMAND_READER_FLYING_THRESHOLD;
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
}
