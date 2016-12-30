package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationForceFailStat extends AbstractMigrationStat {

	public MigrationForceFailStat(MigrationCluster holder) {
		super(holder, MigrationStatus.ForceFail);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		updateDB();
	}

	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Normal.toString());
		getHolder().getClusterService().update(cluster);

		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setStatus(MigrationStatus.ForceFail.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
	}

	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[MigrationForceFail]{}", getHolder().getCurrentCluster().getClusterName());
	}
}
