package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
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
	protected void doRollback() {
		throw new UnsupportedOperationException("migrating, please do tryRollback when partial success");
	}

	@Override
	public void doAction() {

		for(final MigrationShard shard : getHolder().getMigrationShards()) {

			executors.execute(new AbstractExceptionLogTask() {
				@Override
				public void doRun() {
					shard.doMigrate();
				}
			});
		}
	}
	
}
