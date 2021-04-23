package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationMigratingState;

/**
 * @author lishanglin
 * date 2021/3/29
 */
public class MockMigrationMigratingState extends MigrationMigratingState {

    public MockMigrationMigratingState(MigrationCluster holder) {
        super(holder);
        this.setNextAfterSuccess(new MockMigrationPublishState(getHolder()));
    }

    @Override
    public void doAction() {
        super.doAction();
    }

}
