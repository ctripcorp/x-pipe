package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;

public class MigrationTmpEndStat extends AbstractMigrationStat implements MigrationStat {
	
	public MigrationTmpEndStat(MigrationCluster holder) {
		super(holder, MigrationStatus.TmpEnd);
		this.setNextAfterSuccess(new MigrationForceFailStat(getHolder()))
			.setNextAfterFail(new MigrationForceFailStat(getHolder()));
	}

	@Override
	public void action() {
		getHolder().publishStatus(ClusterStatus.TmpMigrated, MigrationStatus.TmpEnd);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
