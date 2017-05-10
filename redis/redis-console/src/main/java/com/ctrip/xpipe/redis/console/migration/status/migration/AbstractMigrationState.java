package com.ctrip.xpipe.redis.console.migration.status.migration;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public abstract class AbstractMigrationState implements MigrationState {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	protected int migrationWaitTimeSeconds = 120;

	private AtomicBoolean hasContine = new AtomicBoolean(false);

	private MigrationCluster holder;
	private MigrationStatus status;
	
	private MigrationState nextAfterSuccess;
	private MigrationState nextAfterFail;

	protected Executor executors;
	
	public AbstractMigrationState(MigrationCluster holder, MigrationStatus status) {
		this.holder = holder;
		this.status = status;
		executors = holder.getMigrationExecutor();
	}

	@Override
	public void action() {
		hasContine.set(false);
		doAction();
	}

	protected abstract void doAction();

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

		if(hasContine.compareAndSet(false, true)){
			logger.info("[MigrationChecking][continue]{}, {}, {}", getHolder().clusterName(), stat, continueProcess);
			getHolder().updateStat(stat);
			if(continueProcess) {
				getHolder().process();
			}
		}else{
			logger.info("[MigrationChecking][already continue]{}, {}, {}", getHolder().clusterName(), stat, continueProcess);
		}
	}

	@Override
	public String toString() {
		return String.format("%s:%s", getClass().getSimpleName(), getHolder() != null ? getHolder().getCurrentCluster().getClusterName() : "");
	}
}
