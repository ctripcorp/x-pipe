package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

import java.util.concurrent.CountDownLatch;

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
	protected void doRollback() {
		throw new  UnsupportedOperationException("already force publish, can not tryRollback");
	}

	@Override
	public void doAction() {

		CountDownLatch latch = new CountDownLatch(getHolder().getMigrationShards().size());

		for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
			executors.execute(new AbstractExceptionLogTask() {
				@Override
				public void doRun() {
					try{
						logger.info("[doOtherDcMigrate][start]{},{}",getHolder().clusterName(),
								migrationShard.getCurrentShard().getShardName());
						migrationShard.doMigrateOtherDc();
						logger.info("[doOtherDcMigrate][done]{},{}",getHolder().clusterName(),
								migrationShard.getCurrentShard().getShardName());
					}finally {
						latch.countDown();
					}
				}
			});
		}
		
		try {
			latch.await();
			updateAndProcess(nextAfterSuccess());
		} catch (InterruptedException e) {
			logger.error("[MigrationForcePublishState][tryAction][Interrupted][will retry]",e);
			updateAndProcess(nextAfterFail());
		}
	}
}
