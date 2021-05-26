package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingState;

/**
 * @author lishanglin
 * date 2021/3/29
 */
public class MockMigrationCheckingState extends MigrationCheckingState {

    public MockMigrationCheckingState(MigrationCluster holder) {
        super(holder);
        this.setNextAfterSuccess(new MockMigrationMigratingState(getHolder()));
    }

    @Override
    public void doAction() {
        MigrationCluster migrationCluster = getHolder();
        doShardCheck(migrationCluster);
    }
}
