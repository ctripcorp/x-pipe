package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

public class MigrationForceFailStat extends AbstractMigrationStat implements MigrationStat {

	public MigrationForceFailStat(MigrationCluster holder) {
		super(holder, MigrationStatus.ForceFail);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Normal.toString());
		getHolder().updateCurrentCluster(cluster);
		
		// Nothing to do
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
