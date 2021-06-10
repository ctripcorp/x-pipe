package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2021/4/19
 */
public class MigrationBuildEventCmdTest extends AbstractConsoleIntegrationTest {

    private BeaconMigrationRequest migrationRequest;

    private MigrationBuildEventCmd buildEventCmd;

    @Autowired
    private MigrationEventDao migrationEventDao;

    @Autowired
    private MigrationEventManager migrationEventManager;

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private MigrationClusterDao migrationClusterDao;

    @Before
    public void setupMigrationBuildEventCmdTest() {
        migrationRequest = new BeaconMigrationRequest();
        buildEventCmd = new MigrationBuildEventCmd(migrationRequest, migrationEventDao, migrationEventManager);
        migrationRequest.setSourceDcTbl(dcService.find("jq"));
        migrationRequest.setTargetDcTbl(dcService.find("oy"));
    }

    @Test
    public void testReuseEvent() throws Throwable {
        migrationRequest.setCurrentMigrationCluster(migrationClusterDao.findByEventIdAndClusterId(1, 2));
        CommandFuture future = buildEventCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        Assert.assertTrue(future.isSuccess());
        Assert.assertEquals(1, migrationRequest.getMigrationEventId());
    }

    @Test
    @DirtiesContext
    public void testCreateNewEvent() throws Throwable {
        migrationRequest.setClusterTbl(clusterService.find("cluster1"));
        CommandFuture future = buildEventCmd.execute();
        waitConditionUntilTimeOut(() -> future.isDone());
        if (!future.isSuccess()) throw future.cause();
        Assert.assertNotNull( migrationEventManager.getEvent(migrationRequest.getMigrationEventId()).getMigrationCluster("cluster1"));
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

}
