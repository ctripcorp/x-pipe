package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.PublishState;

/**
 * @author lishanglin
 * date 2021/6/1
 */
public class MigrationPublishFailState extends AbstractMigrationState implements PublishState {

    public MigrationPublishFailState(MigrationCluster holder) {
        super(holder, MigrationStatus.PublishFail);
    }

    @Override
    public void doAction() {
        updateAndProcess(new MigrationPublishState(getHolder()));
    }

    @Override
    protected void doRollback() {
        rollbackToState(new MigrationPartialSuccessRollBackState(getHolder()));
    }

    @Override
    public void refresh() {
        String clusterName = getHolder().clusterName();
        logger.debug("[MigrationPublishFailState]{}", clusterName);
    }

    @Override
    public void forceEnd() {
        updateAndForceProcess(new MigrationForceEndState(getHolder()));
    }

}
