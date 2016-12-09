package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;

public class MigrationSuccessStat extends AbstractMigrationStat implements MigrationStat {
	
	public MigrationSuccessStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Success);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		getHolder().publishStatus(ClusterStatus.Normal, MigrationStatus.Success);
	}
	
	@Override
	public void refresh() {
		// Nothing to do
	}

}
