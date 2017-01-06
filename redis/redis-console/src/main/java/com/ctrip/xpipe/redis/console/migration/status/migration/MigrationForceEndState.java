package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

/**
 * @author shyin
 *
 * Dec 30, 2016
 */
public class MigrationForceEndState extends AbstractMigrationState {

	public MigrationForceEndState(MigrationCluster holder) {
		super(holder, MigrationStatus.ForceEnd);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}
	
	@Override
	public void action() {
		updateDB();
		
		getHolder().update(getHolder(), getHolder());
	}

	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Normal.toString());
		getHolder().getClusterService().update(cluster);

		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setStatus(MigrationStatus.ForceEnd.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
	}

	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[MigrationForceEnd]{}", getHolder().getCurrentCluster().getClusterName());
	}
}
