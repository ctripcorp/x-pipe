package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;

public class MigrationForcePublishStat extends AbstractMigrationStat implements MigrationStat {

	public MigrationForcePublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.ForcePublish);
		this.setNextAfterSuccess(new MigrationTmpEndStat(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		getHolder().publishStatus(ClusterStatus.TmpMigrated, MigrationStatus.ForcePublish);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
