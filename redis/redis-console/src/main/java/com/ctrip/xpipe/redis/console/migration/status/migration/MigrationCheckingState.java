package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.List;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.ClusterStepResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 *         Dec 8, 2016
 */
public class MigrationCheckingState extends AbstractMigrationState {

	public MigrationCheckingState(MigrationCluster holder) {
		super(holder, MigrationStatus.Checking);
		this.setNextAfterSuccess(new MigrationMigratingState(holder))
			.setNextAfterFail(new MigrationCheckingFailState(holder));
	}

	@Override
	public void doAction() {
		MigrationCluster migrationCluster = getHolder();
		
		List<MigrationShard> migrationShards = migrationCluster.getMigrationShards();
		for (final MigrationShard migrationShard : migrationShards) {

			migrationShard.retry(ShardMigrationStep.CHECK);
			executors.execute(new AbstractExceptionLogTask() {
				@Override
				public void doRun() {
					migrationShard.doCheck();
				}
			});
		}
	}

	@Override
	protected void doRollback() {
		updateAndForceProcess(new MigrationAbortedState(getHolder()));
	}

	@Override
	public void refresh() {

		MigrationCluster holder = getHolder();
		ClusterStepResult clusterStepResult = holder.stepStatus(ShardMigrationStep.CHECK);

		if(clusterStepResult.isStepFinish()){
			if(clusterStepResult.isStepSuccess()){
				logger.debug("[refresh][check success]{}", this);
				updateAndProcess(nextAfterSuccess());
			}else {
				logger.debug("[refresh][check fail]{}", this);
				updateAndStop(nextAfterFail());
			}
		}

	}

}
