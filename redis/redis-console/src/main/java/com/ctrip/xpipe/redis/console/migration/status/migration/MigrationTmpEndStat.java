package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

public class MigrationTmpEndStat extends AbstractMigrationStat {
	
	public MigrationTmpEndStat(MigrationCluster holder) {
		super(holder, MigrationStatus.TmpEnd);
		this.setNextAfterSuccess(new MigrationForceFailStat(getHolder()))
			.setNextAfterFail(new MigrationForceFailStat(getHolder()));
	}

	@Override
	public void action() {
		updateDB();

	}

	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().getClusterService().update(cluster);

		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setStatus(MigrationStatus.TmpEnd.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
