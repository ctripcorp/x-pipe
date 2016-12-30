package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationMigratingStat extends AbstractMigrationMigratingStat {

	public MigrationMigratingStat(MigrationCluster holder) {
		super(holder, MigrationStatus.Migrating);
		this.setNextAfterSuccess(new MigrationPublishStat(holder))
			.setNextAfterFail(new MigrationPartialSuccessStat(holder));
	}

	@Override
	public void action() {
		updateDB();
		
		for(final MigrationShard shard : getHolder().getMigrationShards()) {
			fixedThreadPool.submit(new Runnable() {
				@Override
				public void run() {
					logger.info("[doMigrate][start]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							shard.getCurrentShard().getShardName());
					shard.doMigrate();
					logger.info("[doMigrate][done]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							shard.getCurrentShard().getShardName());
				}
			});
		}
	}
	
	@DalTransaction
	private void updateDB() {
		// Update cluster status
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Migrating.toString());
		getHolder().getClusterService().update(cluster);
		
		// Update migration cluster status
		MigrationClusterTbl migrationCluster = getHolder().getMigrationCluster();
		migrationCluster.setStatus(MigrationStatus.Migrating.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationCluster);
		
		logger.info("[updateDB]Cluster:{}, MigrationCluster:{}", cluster.getClusterName(), migrationCluster);
	}
	
}
