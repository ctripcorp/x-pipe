package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;

public class MigrationInitiatedStat extends AbstractMigrationStat implements MigrationStat {
	
	public MigrationInitiatedStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Initiated);
		this.setNextAfterSuccess(new MigrationCheckingStat(holder))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		// Check cluster status
		ClusterTbl cluster = getHolder().getCurrentCluster();
		if(!ClusterStatus.isSameClusterStatus(cluster.getStatus(), ClusterStatus.Lock)) {
			if(ClusterStatus.isSameClusterStatus(cluster.getStatus(), ClusterStatus.Normal)) {
				logger.info("Cluster:{} Initiated but Unlocked.Lock it now !!!", cluster.getClusterName());
				cluster.setStatus(ClusterStatus.Lock.toString());
				getHolder().updateCurrentCluster(cluster);
				
			} else {
				throw new BadRequestException(String.format("Invalid: cluster %s with status %s", cluster.getClusterName(), cluster.getStatus()));
			}
		}
		
		updateAndProcess(nextAfterSuccess(), true);
	}

	@Override
	public void refresh() {
		// nothing to do
	}
	
}
