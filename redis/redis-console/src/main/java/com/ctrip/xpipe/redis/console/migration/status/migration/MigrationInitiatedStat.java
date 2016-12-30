package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationInitiatedStat extends AbstractMigrationStat {
	
	public MigrationInitiatedStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Initiated);
		this.setNextAfterSuccess(new MigrationCheckingStat(holder))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		// Check cluster status
		ClusterTbl cluster = getHolder().getCurrentCluster();
		ClusterStatus status = ClusterStatus.valueOf(cluster.getStatus());
		if(! status.equals(ClusterStatus.Lock)) {
			if(status.equals(ClusterStatus.Normal)) {
				logger.info("Cluster:{} Initiated but Unlocked.Lock it now !!!", cluster.getClusterName());
				cluster.setStatus(ClusterStatus.Lock.toString());
				getHolder().getClusterService().update(cluster);
			} else {
				throw new IllegalStateException(String.format("Invalid: cluster %s with status %s", cluster.getClusterName(), cluster.getStatus()));
			}
		}
		
		updateAndProcess(nextAfterSuccess(), true);
	}

	@Override
	public void refresh() {
		// nothing to do
		logger.debug("[MigrationInitiatedStat]{}", getHolder().getCurrentCluster().getClusterName());
	}
	
}
