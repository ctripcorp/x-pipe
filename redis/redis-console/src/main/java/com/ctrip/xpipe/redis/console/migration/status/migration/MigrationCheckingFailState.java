package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 *         Dec 8, 2016
 */
public class MigrationCheckingFailState extends AbstractMigrationState {

	public MigrationCheckingFailState(MigrationCluster holder) {
		super(holder, MigrationStatus.CheckingFail);
	}

	@Override
	public void doAction() {
		updateAndProcess(new MigrationCheckingState(getHolder()));
	}

	@Override
	protected void doRollback() {
		rollbackToState(new MigrationAbortedState(getHolder()));
	}

	@Override
	public void refresh() {

	}

}
