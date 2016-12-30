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
public class MigrationCancelledStat extends AbstractMigrationStat implements MigrationStat {

	public MigrationCancelledStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Cancelled);
		this.setNextAfterSuccess(this)
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		try {
			updateDB();
			updateAndProcess(nextAfterSuccess(), false);
		} catch (Exception e) {
			logger.error("[MigrationCancelledStat][updateDB][Failed][will retry]",e);
			updateAndProcess(nextAfterFail(), true);
		}
	}
	
	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Normal.toString());
		getHolder().getClusterService().update(cluster);
		
		MigrationClusterTbl migrationCluster = getHolder().getMigrationCluster();
		migrationCluster.setStatus(MigrationStatus.Cancelled.toString());
		migrationCluster.setEndTime(new Date());
		getHolder().getMigrationService().updateMigrationCluster(migrationCluster);
	}

	@Override
	public void refresh() {
		// Nothing to do
		logger.debug("[MigrationCancelled]{}", getHolder().getCurrentCluster().getClusterName());
	}
}
