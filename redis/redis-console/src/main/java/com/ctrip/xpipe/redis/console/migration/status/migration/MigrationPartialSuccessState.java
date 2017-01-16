package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
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
public class MigrationPartialSuccessState extends AbstractMigrationMigratingState {
	
	public MigrationPartialSuccessState(MigrationCluster holder) {
		super(holder, MigrationStatus.PartialSuccess);
		this.setNextAfterSuccess(new MigrationPublishState(holder))
			.setNextAfterFail(this);
	}

	@Override
	public void action() {
		updateDB();
		
		for(final MigrationShard shard : getHolder().getMigrationShards()) {
			if(!shard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
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
	}
	
	@DalTransaction
	private void updateDB() {
		ClusterTbl cluster = getHolder().getCurrentCluster();
		cluster.setStatus(ClusterStatus.Migrating.toString());
		getHolder().getClusterService().update(cluster);
		
		MigrationClusterTbl migrationClusterTbl = getHolder().getMigrationCluster();
		migrationClusterTbl.setStatus(MigrationStatus.PartialSuccess.toString());
		getHolder().getMigrationService().updateMigrationCluster(migrationClusterTbl);
		
		logger.debug("[updateDB]Cluster:{}, MigrationCluster:{}", cluster, migrationClusterTbl);
	}

}
