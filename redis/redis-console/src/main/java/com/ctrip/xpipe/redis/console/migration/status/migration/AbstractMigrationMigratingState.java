package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author shyin
 *         <p>
 *         Dec 25, 2016
 */
public abstract class AbstractMigrationMigratingState extends AbstractMigrationState {

    private AtomicBoolean doOtherDcMigrate = new AtomicBoolean(false);

    public AbstractMigrationMigratingState(MigrationCluster holder, MigrationStatus status) {
        super(holder, status);
    }

    @Override
    public void refresh() {
        int setUpNewSuccessCnt = 0;
        int currentlyWorkingCnt = 0;
        List<MigrationShard> migrationShards = getHolder().getMigrationShards();
        final int migrationShardsSize = migrationShards.size();

        for (MigrationShard migrationShard : migrationShards) {
            if (migrationShard.getShardMigrationResult().stepTerminated(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
                if (migrationShard.getShardMigrationResult().stepSuccess(ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC)) {
                    ++setUpNewSuccessCnt;
                }
            } else {
                ++currentlyWorkingCnt;
            }
        }

        if (currentlyWorkingCnt == 0) {
            if (setUpNewSuccessCnt == migrationShardsSize) {
                // all success
                int finishedCnt = 0;
                for (MigrationShard migrationShard : migrationShards) {
                    if (migrationShard.getShardMigrationResult().stepTerminated(ShardMigrationStep.MIGRATE)) {
                        ++finishedCnt;
                    }
                }

                if (0 == finishedCnt && doOtherDcMigrate.compareAndSet(false, true)) {
                    doMigrateOtherDc();
                } else if (finishedCnt == migrationShardsSize) {
                    logger.info("[success][continue]{}", getHolder().clusterName());
                    updateAndProcess(nextAfterSuccess());
                }
            } else {
                // any fail
                logger.info("[fail]{}", getHolder().clusterName());
                if (this instanceof MigrationMigratingState) {
                    updateAndProcess(nextAfterFail());
                    return;
                }
                if (this instanceof MigrationPartialSuccessState) {
                    updateAndStop(nextAfterFail());
                    return;
                }
            }
        }
    }

    protected void doMigrateOtherDc() {

        logger.debug("[doMigrateOtherDc]{}", this);

        MigrationCluster migrationCluster = getHolder();
        String clusterName = migrationCluster.clusterName();

        for (MigrationShard migrationShard : migrationCluster.getMigrationShards()) {
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                public void doRun() {
                    String shardName = migrationShard.shardName();
                    logger.info("[doOtherDcMigrate][start]{},{}", clusterName, shardName);
                    migrationShard.doMigrateOtherDc();
                    logger.info("[doOtherDcMigrate][done]{},{}", clusterName, shardName);
                }
            });
        }
    }
}
