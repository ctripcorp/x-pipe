package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigrationNotSuccessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2021/4/19
 */
public class MigrationDoExecuteCmdTest extends AbstractConsoleIntegrationTest {

    private BeaconMigrationRequest migrationRequest;

    private MigrationDoExecuteCmd doExecuteCmd;

    @Autowired
    private MigrationEventManager migrationEventManager;

    @Autowired
    private ClusterService clusterService;

    @Before
    public void setupMigrationDoExecuteCmdTest() {
        migrationRequest = new BeaconMigrationRequest();
        doExecuteCmd = new MigrationDoExecuteCmd(migrationRequest, migrationEventManager, executors);
    }

    @Test(expected = ClusterMigrationNotSuccessException.class)
    public void testExecuteFail() throws Throwable {
        migrationRequest.setMigrationEventId(1);
        migrationRequest.setClusterTbl(clusterService.find("cluster2"));
        CommandFuture future = doExecuteCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertFalse(future.isSuccess());
        throw future.cause();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

}
