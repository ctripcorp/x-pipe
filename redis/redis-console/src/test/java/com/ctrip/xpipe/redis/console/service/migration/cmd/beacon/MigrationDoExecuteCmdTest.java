package com.ctrip.xpipe.redis.console.service.migration.cmd.beacon;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.migration.command.ReactorMigrationCommandBuilderImpl;
import com.ctrip.xpipe.redis.console.migration.manager.MigrationEventManager;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterMigrationNotSuccessException;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;

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

    @MockBean
    private ReactorMigrationCommandBuilderImpl reactorMigrationCommandBuilder;

    @Before
    public void setupMigrationDoExecuteCmdTest() {
        migrationRequest = new BeaconMigrationRequest();
        doExecuteCmd = new MigrationDoExecuteCmd(migrationRequest, migrationEventManager, executors);
        Mockito.when(reactorMigrationCommandBuilder.buildDcCheckCommand(anyString(), anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> failCheckCommand());
    }

    private AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage> failCheckCommand() {
        return new AbstractCommand<MetaServerConsoleService.PrimaryDcCheckMessage>() {
            @Override
            public String getName() {
                return "Mocked-MigrationDoExecute-CheckFail";
            }

            @Override
            protected void doExecute() throws Exception {
                future().setSuccess(new MetaServerConsoleService.PrimaryDcCheckMessage(
                        MetaServerConsoleService.PRIMARY_DC_CHECK_RESULT.FAIL, "mocked check fail"));
            }

            @Override
            protected void doReset() {
            }
        };
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
