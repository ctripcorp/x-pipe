package com.ctrip.xpipe.redis.console.migration.status.migration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.utils.XpipeThreadFactory;

/**
 * @author shyin
 *
 * Dec 25, 2016
 */
public abstract class AbstractMigrationMigratingState extends AbstractMigrationState{

	protected ExecutorService fixedThreadPool;
	
    public AbstractMigrationMigratingState(MigrationCluster holder, MigrationStatus status) {
        super(holder, status);
        
        int threadSize = holder.getMigrationShards().size() == 0 ? 1 : holder.getMigrationShards().size();
        fixedThreadPool = Executors.newFixedThreadPool(threadSize, XpipeThreadFactory.create(getClass().toString()));
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
    			if(0 == finishedCnt) {
    				doMigrateOtherDc();
    			} else if(finishedCnt == getHolder().getMigrationShards().size()) {
    				logger.info("[{}][success][continue]{}",getClass(), getHolder().getCurrentCluster().getClusterName());
                    updateAndProcess(nextAfterSuccess(), true);
    			}
    		} else {
    			// any fail
    			logger.info("[{}][fail]{}",getClass(), getHolder().getCurrentCluster().getClusterName());
                updateAndProcess(nextAfterFail(), false);
    		}
    	}
    }
    
    protected void doMigrateOtherDc() {
    	for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
			fixedThreadPool.submit(new Runnable() {
				@Override
				public void run() {
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
