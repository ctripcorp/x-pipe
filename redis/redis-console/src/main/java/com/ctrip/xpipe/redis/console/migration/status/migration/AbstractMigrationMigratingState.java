package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *
 * Dec 25, 2016
 */
public abstract class AbstractMigrationMigratingState extends AbstractMigrationState{

	private boolean doOtherDcMigrate;
	
    public AbstractMigrationMigratingState(MigrationCluster holder, MigrationStatus status) {
        super(holder, status);
        doOtherDcMigrate = false;
    }

    @Override
    public void refresh() {
    	int setUpNewSuccessCnt = 0;
    	int currentlyWorkingCnt = 0;
    	for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
    		if(migrationShard.getShardMigrationResult().stepTerminated(ShardMigrationResult.ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
    			if(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationResult.ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
					++setUpNewSuccessCnt;
				}
    		} else {
    			++currentlyWorkingCnt;
    		}
    	}
    	
    	if(currentlyWorkingCnt == 0) {
    		if(setUpNewSuccessCnt == getHolder().getMigrationShards().size()) {
    			// all success
    			int finishedCnt = 0;
    			for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
    				if(migrationShard.getShardMigrationResult().stepTerminated(ShardMigrationResult.ShardMigrationStep.MIGRATE)) {
    					++finishedCnt;
    				}
    			}
    			
    			if(0 == finishedCnt && !doOtherDcMigrate) {
    				doMigrateOtherDc();
    				doOtherDcMigrate = true;
    			} else if(finishedCnt == getHolder().getMigrationShards().size()) {
    				logger.info("[success][continue]{}", getHolder().getCurrentCluster().getClusterName());
                    updateAndProcess(nextAfterSuccess());
    			}
    		} else {
    			// any fail
    			logger.info("[fail]{}", getHolder().getCurrentCluster().getClusterName());
    			if(this instanceof MigrationMigratingState) {
    				updateAndProcess(nextAfterFail());
    				return;
    			} 
    			if(this instanceof MigrationPartialSuccessState){
    				updateAndStop(nextAfterFail());
    				return;
    			}
    		}
    	}
    }
    
    protected void doMigrateOtherDc() {
    	for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
			executors.execute(new AbstractExceptionLogTask() {
				@Override
				public void doRun() {
					logger.info("[doOtherDcMigrate][start]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							migrationShard.getCurrentShard().getShardName());
					migrationShard.doMigrateOtherDc();
					logger.info("[doOtherDcMigrate][done]{},{}",getHolder().getCurrentCluster().getClusterName(), 
							migrationShard.getCurrentShard().getShardName());
				}
			});
		}
    }
}
