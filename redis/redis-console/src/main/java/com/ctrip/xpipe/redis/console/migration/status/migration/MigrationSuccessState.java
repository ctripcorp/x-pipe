package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationSuccessState extends AbstractMigrationState {
	
	public MigrationSuccessState(MigrationCluster holder) {
		super(holder, MigrationStatus.Success);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void doAction() {
		getHolder().update(getHolder(), getHolder());
	}

	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[MigrationSuccess]{}", getHolder().getCurrentCluster().getClusterName());
	}

}
