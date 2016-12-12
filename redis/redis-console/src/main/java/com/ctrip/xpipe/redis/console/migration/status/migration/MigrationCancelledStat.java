package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationCancelledStat extends AbstractMigrationStat implements MigrationStat {

	public MigrationCancelledStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Cancelled);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		getHolder().cancel();
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
