package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author shyin
 *         <p>
 *         Dec 8, 2016
 */
public class MigrationInitiatedState extends AbstractMigrationState {

    public MigrationInitiatedState(MigrationCluster holder) {
        super(holder, MigrationStatus.Initiated);
        this.setNextAfterSuccess(new MigrationCheckingState(holder))
                .setNextAfterFail(this);
    }

    @Override
    public void doAction() {
        // Check cluster status
        updateAndProcess(nextAfterSuccess());
    }

    @Override
    protected void doRollback() {

        rollbackToState(new MigrationAbortedState(getHolder()));
    }

    @Override
    public void refresh() {
        // nothing to do
        logger.debug("[MigrationInitiatedStat]{}", getHolder().clusterName());
    }

}
