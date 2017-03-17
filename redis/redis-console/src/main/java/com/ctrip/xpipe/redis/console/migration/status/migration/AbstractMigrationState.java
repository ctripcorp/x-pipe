package com.ctrip.xpipe.redis.console.migration.status.migration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public abstract class AbstractMigrationState implements MigrationState {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	protected int migrationWaitTimeSeconds = 120;    
	
	private MigrationCluster holder;
	private MigrationStatus status;
	
	private MigrationState nextAfterSuccess;
	private MigrationState nextAfterFail;
	
	public AbstractMigrationState(MigrationCluster holder, MigrationStatus status) {
		this.holder = holder;
		this.status = status;
	}
	
	public MigrationCluster getHolder() {
		return holder;
	}
	
	@Override
	public MigrationStatus getStatus() {
		return status;
	}
	
	@Override
	public MigrationState nextAfterSuccess() {
		return nextAfterSuccess;
	}
	
	public AbstractMigrationState setNextAfterSuccess(MigrationState nextAfterSuccess) {
		this.nextAfterSuccess = nextAfterSuccess;
		return this;
	}
	
	@Override
	public MigrationState nextAfterFail() {
		return nextAfterFail;
	}
	
	public AbstractMigrationState setNextAfterFail(MigrationState nextAfterFail) {
		this.nextAfterFail = nextAfterFail;
		return this;
	}
	
	public void updateAndProcess(MigrationState stat, boolean continueProcess) {
		getHolder().updateStat(stat);
		if(continueProcess) {
			getHolder().process();
		} 
	}
	
}
