package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.redis.console.migration.command.MigrationCommandBuilder;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedState;

/**
 * @author lishanglin
 * date 2021/3/29
 */
public class MockMigrationInitiatedState extends MigrationInitiatedState {

    private MigrationCommandBuilder builder;

    public MockMigrationInitiatedState(MigrationCluster holder) {
        this(holder, null);
    }

    public MockMigrationInitiatedState(MigrationCluster holder, MigrationCommandBuilder injectCommandBuilder) {
        super(holder);
        this.setNextAfterSuccess(new MockMigrationCheckingState(getHolder()));
        if (null != injectCommandBuilder) this.builder = injectCommandBuilder;
        else this.builder = new MockMigrationCommandBuilder();
    }

    @Override
    public void doAction() {
        MigrationCluster migrationCluster = getHolder();
        migrationCluster.getMigrationShards().forEach(migrationShard -> {
            ((DefaultMigrationShard)migrationShard).setCommandBuilder(builder);
        });

        updateAndForceProcess(nextAfterSuccess());
    }

}
