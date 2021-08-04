package com.ctrip.xpipe.redis.console.migration;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2021/8/4
 */
public class MultiClusterMigrationTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationEventManager migrationEventManager;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-multi-cluster.sql");
    }

    @Test
    public void testMigrationClusterRollback() throws Exception {
        MigrationEvent migrationEvent =  migrationEventManager.getEvent(1);
        migrationEvent.getMigrationCluster(1).rollback();
        waitConditionUntilTimeOut(() -> migrationEvent.getMigrationCluster(1).getStatus().isTerminated());
        Assert.assertEquals(MigrationStatus.Initiated, migrationEvent.getMigrationCluster(2).getStatus());
        Assert.assertEquals(MigrationStatus.Initiated, migrationEvent.getMigrationCluster(3).getStatus());
    }

}
