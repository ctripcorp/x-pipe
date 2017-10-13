package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.PartialSuccessState;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public class MigrationPartialSuccessState extends AbstractMigrationMigratingState implements PartialSuccessState{

    public MigrationPartialSuccessState(MigrationCluster holder) {
        super(holder, MigrationStatus.PartialSuccess);
        this.setNextAfterSuccess(new MigrationPublishState(holder))
                .setNextAfterFail(this);
    }

    @Override
    public void doAction() {

        for (final MigrationShard shard : getHolder().getMigrationShards()) {

            ShardMigrationResult shardMigrationResult = shard.getShardMigrationResult();
            if (!shardMigrationResult.stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
                shardMigrationResult.stepRetry(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC);

                String clusterName = getHolder().clusterName();
                String shardName = shard.shardName();
                logger.info("[doAction][execute]{}, {}", clusterName, shardName);
                executors.execute(new AbstractExceptionLogTask() {

                    @Override
                    public void doRun() {
                        logger.info("[doMigrate][start]{},{}", clusterName, shardName);
                        shard.doMigrate();
                        logger.info("[doMigrate][done]{},{}", clusterName, shardName);
                    }
                });
            }
        }
    }


    @Override
    protected void doRollback() {
        rollbackToState(new MigrationPartialSuccessRollBackState(getHolder()));
    }

    @Override
    public void forcePublish() {
        updateAndForceProcess(new MigrationForcePublishState(getHolder()));
    }
}
