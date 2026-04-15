package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

public class MigrationPreMigratingState extends AbstractMigrationState {

    public MigrationPreMigratingState(MigrationCluster holder) {
        super(holder, MigrationStatus.PreMigrating);
        this.setNextAfterSuccess(new MigrationMigratingState(holder))
                .setNextAfterFail(new MigrationMigratingState(holder));
    }

    @Override
    protected void doRollback() {
        rollbackToState(new MigrationAbortedState(getHolder()));
    }

    @Override
    protected void doAction() {
        MigrationCluster migrationCluster = getHolder();
        if (!migrationCluster.getMigrationService().shouldMigrateSentinelBeacon(migrationCluster)) {
            migrationCluster.updateStepResultForAllShards(ShardMigrationStep.PRE_MIGRATING, true,
                    "no beacon");
            updateAndProcess(nextAfterSuccess());
            return;
        }
        RetMessage result = migrationCluster.getMigrationService().preMigrateSentinelBeacon(migrationCluster);
        boolean success = result != null && result.getState() == RetMessage.SUCCESS_STATE;
        String message = result == null ? "sentinel beacon pre migrate return null" : result.getMessage();
        migrationCluster.updateStepResultForAllShards(ShardMigrationStep.PRE_MIGRATING, success, message);
        // pre-migrate beacon operation failure should not block migration flow
        updateAndProcess(nextAfterSuccess());
    }

    @Override
    public void refresh() {
        // nothing to do, state transitions in doAction immediately
    }
}
