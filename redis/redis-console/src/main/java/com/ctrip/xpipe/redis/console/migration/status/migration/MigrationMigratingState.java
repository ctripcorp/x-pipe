package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 * Dec 8, 2016
 */
public class MigrationMigratingState extends AbstractMigrationMigratingState {

	public MigrationMigratingState(MigrationCluster holder) {
		super(holder, MigrationStatus.Migrating);
		this.setNextAfterSuccess(new MigrationPublishState(holder))
			.setNextAfterFail(new MigrationPartialSuccessState(holder));
	}

	@Override
	public void action() {
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
	
}
