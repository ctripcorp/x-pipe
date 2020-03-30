package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.AbstractConsoleH2DbTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.when;

public class DefaultCheckMigrationCommandBuilderTest extends AbstractConsoleH2DbTest {

    @Mock
    private ClusterService clusterService;

    @Mock
    private OuterClientService outerClientService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private DcService dcService;

    private String clusterId = "cluster1", shardId = "shard1";

    private DefaultCheckMigrationCommandBuilder builder;

    private Server server;

    @BeforeClass
    public static void beforeDefaultCheckMigrationCommandBuilderTestClass() {
        AbstractService.DEFAULT_SO_TIMEOUT = 10;
    }

    @Before
    public void beforeDefaultCheckMigrationCommandBuilderTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(consoleConfig.getClusterShardForMigrationSysCheck()).thenReturn(Pair.from(clusterId, shardId));
        when(consoleConfig.getMetaservers()).thenReturn("{\n" +
                "    \"localmeta\": \"http://127.0.0.1:54321\"\n" +
                "}");
        builder = new DefaultCheckMigrationCommandBuilder(scheduled, dcService, clusterService, outerClientService, consoleConfig);
    }

    @After
    public void afterDefaultCheckMigrationCommandBuilderTest() throws Exception {
        if(server != null) {
            server.stop();
        }
    }

    @Test
    public void testCheckCommand() {
        Assert.assertTrue(builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_OUTER_CLIENT) instanceof DefaultCheckMigrationCommandBuilder.CheckOuterClientCommand);
        Assert.assertTrue(builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER) instanceof DefaultCheckMigrationCommandBuilder.CheckMetaServerCommand);
    }

    @Test(expected = ExecutionException.class)
    public void testCheckDataBase() throws ExecutionException, InterruptedException {
        when(clusterService.find(clusterId)).thenReturn(null);
        builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_DATA_BASE).execute().get();
    }

    @Test
    public void testCheckDataBaseWithResponse() throws ExecutionException, InterruptedException {
        when(clusterService.find(clusterId)).thenReturn(new ClusterTbl().setClusterName(clusterId).setId(1));
        RetMessage message = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_DATA_BASE).execute().get();
        Assert.assertEquals(RetMessage.SUCCESS_STATE, message.getState());
        logger.info("{}", message);
    }

    @Test
    public void testCheckOuterClient() throws Exception {
        OuterClientService.ClusterInfo outerCluster = new OuterClientService.ClusterInfo();
        outerCluster.setName(clusterId);
        when(outerClientService.getClusterInfo(clusterId)).thenReturn(outerCluster);
        RetMessage message = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_OUTER_CLIENT).execute().get();
        Assert.assertEquals(RetMessage.SUCCESS_STATE, message.getState());
    }

    @Test
    @Ignore
    public void testCheckOuterClientManually() throws Exception {
        builder = new DefaultCheckMigrationCommandBuilder(scheduled, dcService, clusterService, OuterClientService.DEFAULT, consoleConfig);
        OuterClientService.ClusterInfo clusterInfo = OuterClientService.DEFAULT.getClusterInfo("cluster_shyin");
        logger.info("{}", clusterInfo);
    }


    @Test
    public void testCheckMetaServer() throws Exception {
        server = startServer(54321, "test");
        when(dcService.findClusterRelatedDc(clusterId)).thenReturn(Lists.newArrayList(new DcTbl().setDcName("localmeta")));
        Command<RetMessage> command = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER);
        RetMessage message = command.execute().get();
        logger.info("");
        logger.info("{}", message.getMessage());
        Assert.assertEquals(RetMessage.FAIL_STATE, message.getState());
    }

    @Test
    @Ignore
    public void testCheckMetaServerManually() throws Exception {
        String metaServers = "{\n" +
                "    \"NTGXH\": \"http://xpipe.meta.fws.qa.nt.ctripcorp.com\",\n" +
                "    \"FAT\": \"http://xpipe.meta.fat500.qa.nt.ctripcorp.com\",\n" +
                "    \"FAT-AWS\": \"http://10.2.131.44:8080\"\n" +
                "}";
        when(consoleConfig.getMetaservers()).thenReturn(metaServers);
        when(consoleConfig.getClusterShardForMigrationSysCheck()).thenReturn(Pair.from("cluster_shyin", "shard1"));
        builder = new DefaultCheckMigrationCommandBuilder(scheduled, dcService, clusterService, outerClientService, consoleConfig);
        RetMessage message = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER).execute().get();
        logger.info("{}", message);
    }

}