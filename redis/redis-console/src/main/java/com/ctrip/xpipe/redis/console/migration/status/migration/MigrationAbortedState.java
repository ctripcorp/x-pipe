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
		throw new UnsupportedOperationException("tryRollback success, can not tryRollback:" + getStatus());
	}

	@Override
	public void doAction() {
		try {
			getHolder().update(getHolder(), getHolder());
		}finally {
			markDone();
		}
	}
	
	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[MigrationCancelled]{}", getHolder().clusterName());
	}
}
