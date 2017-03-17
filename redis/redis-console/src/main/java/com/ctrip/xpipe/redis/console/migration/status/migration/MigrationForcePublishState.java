package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.concurrent.CountDownLatch;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 * Jan 3, 2017
 */
public class MigrationForcePublishState extends AbstractMigrationMigratingState {

	public MigrationForcePublishState(MigrationCluster holder) {
		super(holder, MigrationStatus.Migrating);
		this.setNextAfterSuccess(new MigrationPublishState(holder))
			.setNextAfterFail(this);
	}

	
	@Override
	public void action() {
		CountDownLatch latch = new CountDownLatch(getHolder().getMigrationShards().size());
		for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
			fixedThreadPool.submit(new Runnable() {
				@Override
				public void run() {
					logger.info("[doOtherDcMigrate][start]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							migrationShard.getCurrentShard().getShardName());
					migrationShard.doMigrateOtherDc();
					latch.countDown();
					logger.info("[doOtherDcMigrate][done]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							migrationShard.getCurrentShard().getShardName());
				}
			});
		}
		
		try {
			latch.await();
			updateAndProcess(nextAfterSuccess(), true);
		} catch (InterruptedException e) {
			logger.error("[MigrationForcePublishState][action][Interrupted][will retry]",e);
			updateAndProcess(nextAfterFail(), true);
		}
	}
}
