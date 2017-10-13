package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.ClusterStepResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

import java.util.List;

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

		if(!doCheckClientService(migrationCluster)){
		    return;
        }

		doShardCheck(migrationCluster);
	}

    private boolean doCheckClientService(MigrationCluster migrationCluster) {


        String clusterName = migrationCluster.clusterName();
        OuterClientService outerClientService = migrationCluster.getOuterClientService();
        String failMesage = "";

        logger.info("[doCheckClientService]{}", clusterName);

        try {
            OuterClientService.ClusterInfo clusterInfo = outerClientService.getClusterInfo(clusterName);
            //simple check
            if(clusterInfo != null && clusterInfo.getGroups() != null && clusterInfo.getGroups().size() > 0){
                return true;
            }
            failMesage = String.format("%s FAIL, cluster:%s, info empty:%s", outerClientService.serviceName(), clusterName, clusterInfo);
        } catch (Exception e) {
            logger.error("[doCheckClientService]" + clusterName, e);
            failMesage = String.format("%s FAIL, cluster:%s, error message:%s", outerClientService.serviceName(), clusterName, e.getMessage());
        }
        migrationCluster.markCheckFail(failMesage);
        return false;
    }

    private void doShardCheck(MigrationCluster migrationCluster) {
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
		rollbackToState(new MigrationAbortedState(getHolder()));
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
