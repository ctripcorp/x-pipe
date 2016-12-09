package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;

public class MigrationForceFailStat extends AbstractMigrationStat implements MigrationStat {

	public MigrationForceFailStat(MigrationCluster holder) {
		super(holder, MigrationStatus.ForceFail);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		getHolder().publishStatus(ClusterStatus.Normal, MigrationStatus.ForceFail);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
