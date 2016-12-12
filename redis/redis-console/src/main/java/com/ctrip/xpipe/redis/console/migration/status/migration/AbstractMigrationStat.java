package com.ctrip.xpipe.redis.console.migration.status.migration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public abstract class AbstractMigrationStat implements MigrationStat {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private MigrationCluster holder;
	private MigrationStatus status;
	
	private MigrationStat nextAfterSuccess;
	private MigrationStat nextAfterFail;
	
	public AbstractMigrationStat(MigrationCluster holder, MigrationStatus status) {
		this.holder = holder;
		this.status = status;
	}
	
	public MigrationCluster getHolder() {
		return holder;
	}
	
	@Override
	public MigrationStatus getStat() {
		return status;
	}
	
	@Override
	public MigrationStat nextAfterSuccess() {
		return nextAfterSuccess;
	}
	
	public AbstractMigrationStat setNextAfterSuccess(MigrationStat nextAfterSuccess) {
		this.nextAfterSuccess = nextAfterSuccess;
		return this;
	}
	
	@Override
	public MigrationStat nextAfterFail() {
		return nextAfterFail;
	}
	
	public AbstractMigrationStat setNextAfterFail(MigrationStat nextAfterFail) {
		this.nextAfterFail = nextAfterFail;
		return this;
	}
	
	public void updateAndProcess(MigrationStat stat, boolean continueProcess) {
		getHolder().updateStat(stat);
		if(continueProcess) {
			getHolder().process();
		} 
	}
	
}
