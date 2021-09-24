package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.ForceProcessAbleState;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public class MigrationPartialSuccessState extends AbstractMigrationMigratingState implements ForceProcessAbleState {

    public MigrationPartialSuccessState(MigrationCluster holder) {
        super(holder, MigrationStatus.PartialSuccess);
        this.setNextAfterSuccess(new MigrationPublishState(holder))
                .setNextAfterFail(new MigrationPartialRetryFailState(holder));
    }

    @Override
    public void doAction() {
        String clusterName = getHolder().clusterName();
        List<MigrationShard> failedShards = getHolder().getMigrationShards().stream().filter(shard ->
                !shard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)
        ).collect(Collectors.toList());

        if (failedShards.isEmpty()) {
            logger.info("[doAction][{}] no failed shards and refresh", clusterName);
            refresh();
        } else {
            failedShards.forEach(shard -> shard.getShardMigrationResult().stepRetry(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC));
            failedShards.forEach(shard -> {
                String shardName = shard.shardName();
                logger.info("[doAction][execute]{}, {}", clusterName, shardName);
                executors.execute(new AbstractExceptionLogTask() {
                    @Override
                    public void doRun() {
                        shard.doMigrate();
                    }
                });
            });
        }
    }


    @Override
    protected void doRollback() {
        rollbackToState(new MigrationPartialSuccessRollBackState(getHolder()));
    }

    @Override
    public void updateAndForceProcess() {
        updateAndForceProcess(new MigrationForcePublishState(getHolder()));
    }
}
