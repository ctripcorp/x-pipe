package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.Date;

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
public class MigrationPublishStat extends AbstractMigrationStat {
	
	public MigrationPublishStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Publish);
		this.setNextAfterSuccess(new MigrationSuccessStat(getHolder()))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		updateDB();
		updateAndProcess(nextAfterSuccess(), true);
	}

	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setActivedcId(getHolder().getMigrationCluster().getDestinationDcId());
		cluster.setStatus(ClusterStatus.TmpMigrated.toString());
		getHolder().getClusterService().update(cluster);

		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setEndTime(new Date());
		migrationClusterTbl.setStatus(MigrationStatus.Publish.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
		
		
	}

	@Override
	public void refresh() {
		// Nothing to do
	}
}
