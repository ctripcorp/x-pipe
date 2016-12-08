package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

public class MigrationPublishStat extends AbstractMigrationStat implements MigrationStat {
	
	public MigrationPublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Publish);
		this.setNextAfterSuccess(new MigrationSuccessStat(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		// TODO : publish action(keeper records persistent)
		
		
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().updateCurrentCluster(cluster);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
