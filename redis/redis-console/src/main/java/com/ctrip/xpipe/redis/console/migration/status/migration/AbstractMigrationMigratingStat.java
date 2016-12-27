package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.command.result.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;

/**
 * @author shyin
 *
 * Dec 25, 2016
 */
public abstract class AbstractMigrationMigratingStat extends AbstractMigrationStat{

    public AbstractMigrationMigratingStat(MigrationCluster holder, MigrationStatus status) {
        super(holder, status);
    }

    @Override
    public void refresh() {
        int successCnt = 0;
        int currentWorkingCnt = 0;

        for(MigrationShard migrationShard : getHolder().getMigrationShards()) {
            if(migrationShard.getShardMigrationResult().stepTerminated(ShardMigrationResult.ShardMigrationStep.MIGRATE)) {
                if(migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationResult.ShardMigrationStep.MIGRATE)) {
                    ++successCnt;
                }
            } else {
                ++currentWorkingCnt;
            }
        }

        if(currentWorkingCnt == 0) {
            if (successCnt == getHolder().getMigrationShards().size()) {
                logger.info("[{}][success][continue]{}",getClass(), getHolder().getCurrentCluster().getClusterName());
                updateAndProcess(nextAfterSuccess(), true);
            } else {
                logger.info("[{}][fail]{}",getClass(), getHolder().getCurrentCluster().getClusterName());
                updateAndProcess(nextAfterFail(), false);
            }
        } else {
            // Still migrating , Nothing to do
        }
    }
}
