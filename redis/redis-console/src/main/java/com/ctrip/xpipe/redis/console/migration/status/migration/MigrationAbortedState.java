package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationAbortedState extends AbstractMigrationState implements MigrationState {

	public MigrationAbortedState(MigrationCluster holder) {
		super(holder, MigrationStatus.Aborted);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	protected void doRollback() {
		throw new UnsupportedOperationException("rollback success, can not rollback:" + getStatus());
	}

	@Override
	public void doAction() {
		getHolder().update(getHolder(), getHolder());
	}
	
	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[MigrationCancelled]{}", getHolder().clusterName());
	}
}
