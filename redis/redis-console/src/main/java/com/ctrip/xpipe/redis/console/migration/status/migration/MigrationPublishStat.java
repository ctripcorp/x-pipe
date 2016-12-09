package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;

public class MigrationPublishStat extends AbstractMigrationStat implements MigrationStat {
	
	public MigrationPublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Publish);
		this.setNextAfterSuccess(new MigrationSuccessStat(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		getHolder().publishStatus(ClusterStatus.TmpMigrated, MigrationStatus.Publish);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
