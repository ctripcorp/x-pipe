package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

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
	public void doAction() {
		for(final MigrationShard shard : getHolder().getMigrationShards()) {
			if(!shard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
				executors.submit(new AbstractExceptionLogTask() {
					@Override
					public void doRun() {
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
}
