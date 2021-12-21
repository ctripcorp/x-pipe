package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.redis.console.AbstractConsoleDbTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.when;

public class DefaultCheckMigrationCommandBuilderTest extends AbstractConsoleDbTest {

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

    private static int originTimeout;

    @BeforeClass
    public static void beforeDefaultCheckMigrationCommandBuilderTestClass() {
        originTimeout = AbstractService.DEFAULT_SO_TIMEOUT;
        AbstractService.DEFAULT_SO_TIMEOUT = 10;
    }

    @AfterClass
    public static void afterDefaultCheckMigrationCommandBuilderTestClass() {
        AbstractService.DEFAULT_SO_TIMEOUT = originTimeout;
    }

    @Before
    public void beforeDefaultCheckMigrationCommandBuilderTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(consoleConfig.getClusterShardForMigrationSysCheck()).thenReturn(Pair.from(clusterId, shardId));
        when(consoleConfig.getMetaservers()).thenReturn(Collections.singletonMap("localmeta", "http://127.0.0.1:54321"));
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

        when(consoleConfig.getMetaservers()).thenReturn(new HashMap<String, String>() {{
            put("NTGXH", "http://xpipe.meta.fws.qa.nt.ctripcorp.com");
            put("FAT", "http://xpipe.meta.fat500.qa.nt.ctripcorp.com");
            put("FAT-AWS", "http://10.2.131.44:8080");
        }});
        when(consoleConfig.getClusterShardForMigrationSysCheck()).thenReturn(Pair.from("cluster_shyin", "shard1"));
        builder = new DefaultCheckMigrationCommandBuilder(scheduled, dcService, clusterService, outerClientService, consoleConfig);
        RetMessage message = builder.checkCommand(CHECK_MIGRATION_SYSTEM_STEP.CHECK_METASERVER).execute().get();
        logger.info("{}", message);
    }

}