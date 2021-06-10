package com.ctrip.xpipe.redis.console.migration.status.migration;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;

/**
 * @author lishanglin
 * date 2021/6/1
 */
public class MigrationPartialSuccessRollBackFailState extends AbstractMigrationState {

    public MigrationPartialSuccessRollBackFailState(MigrationCluster holder) {
        super(holder, MigrationStatus.RollBackFail);
    }

    @Override
    public void doAction() {
        updateAndProcess(new MigrationPartialSuccessRollBackState(getHolder()));
    }

    @Override
    protected void doRollback() {
        throw new UnsupportedOperationException("already rollbacking, can not tryRollback rollback");
    }

    @Override
    public void refresh() {
        String clusterName = getHolder().clusterName();
        logger.debug("[MigrationPartialSuccessRollBackFailState]{}", clusterName);
    }

}
