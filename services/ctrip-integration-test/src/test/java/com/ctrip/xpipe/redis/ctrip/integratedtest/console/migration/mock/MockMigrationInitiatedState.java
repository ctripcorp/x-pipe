package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedState;

/**
 * @author lishanglin
 * date 2021/3/29
 */
public class MockMigrationInitiatedState extends MigrationInitiatedState {

    public MockMigrationInitiatedState(MigrationCluster holder) {
        super(holder);
    }

    @Override
    public void doAction() {
        updateAndForceProcess(new MockMigrationCheckingState(getHolder()));
    }

}
