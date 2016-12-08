package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

public class MigrationTmpEndStat extends AbstractMigrationStat implements MigrationStat {
	
	public MigrationTmpEndStat(MigrationCluster holder) {
		super(holder, MigrationStatus.TmpEnd);
		this.setNextAfterSuccess(new MigrationForceFailStat(getHolder()))
			.setNextAfterFail(new MigrationForceFailStat(getHolder()));
	}

	@Override
	public void action() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().updateCurrentCluster(cluster);
	
		// Nothing to do
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
