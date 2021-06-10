package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.ForceProcessAbleState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author lishanglin
 * date 2021/6/1
 */
public class MigrationPartialRetryFailState extends AbstractMigrationState implements ForceProcessAbleState {

    public MigrationPartialRetryFailState(MigrationCluster holder) {
        super(holder, MigrationStatus.PartialRetryFail);
    }

    @Override
    public void doAction() {
        updateAndProcess(new MigrationPartialSuccessState(getHolder()));
    }

    @Override
    protected void doRollback() {
        rollbackToState(new MigrationPartialSuccessRollBackState(getHolder()));
    }

    @Override
    public void refresh() {
        String clusterName = getHolder().clusterName();
        logger.debug("[MigrationPartialSuccessRetryFailState]{}", clusterName);
    }

    @Override
    public void updateAndForceProcess() {
        updateAndForceProcess(new MigrationForcePublishState(getHolder()));
    }

}
