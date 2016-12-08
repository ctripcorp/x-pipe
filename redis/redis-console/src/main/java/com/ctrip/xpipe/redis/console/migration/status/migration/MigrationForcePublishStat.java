package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

public class MigrationForcePublishStat extends AbstractMigrationStat implements MigrationStat {

	public MigrationForcePublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.ForcePublish);
		this.setNextAfterSuccess(new MigrationTmpEndStat(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		// TODO publish
		
		
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().updateCurrentCluster(cluster);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
