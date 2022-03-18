package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.controller;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckActionTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.aggregator.CurrentDcSentinelHelloAggregationCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeCommand;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.IoAction;
import com.ctrip.xpipe.simpleserver.IoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CurrentDcSentinelHelloCheckControllerTest extends AbstractCheckerTest {

    private CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus jqMaster;
    private CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus jqSlave;
    private CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus oyMaster;
    private CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus oySlave;

    private RedisMeta jqMasterMeta = new RedisMeta().setIp("jqMasterMeta");
    private RedisMeta jqSlaveMeta = new RedisMeta().setIp("jqSlaveMeta").setMaster("0.0.0.0:0");
    private RedisMeta oyMasterMeta = new RedisMeta().setIp("oyMasterMeta");
    private RedisMeta oySlaveMeta = new RedisMeta().setIp("oySlaveMeta").setMaster("0.0.0.0:0");

    @Mock
    protected CheckerDbConfig config;

    @Mock
    protected PersistenceCache persistence;

    private int sentinelCollectInterval = 600;

    private int sentinelCheckInterval = 800;

    private int sentinelSubTimeout = 500;

    @InjectMocks
    private CurrentDcSentinelHelloCheckController checkActionController;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private CurrentDcSentinelHelloCollector sentinelHelloCollector;

    @Mock
    private HealthCheckInstanceManager instanceManager;

    private SentinelHelloCheckAction checkAction;

    private ClusterHealthCheckInstance instance;

    private CurrentDcSentinelHelloAggregationCollector downgradeController;

    private static final String clusterName = "cluster";

    private static final String shardName = "shard";

    @Before
    public void beforeSentinelHelloActionDowngradeTest() throws Exception {
        instance = newRandomClusterHealthCheckInstance(null, ClusterType.BI_DIRECTION);
        ((DefaultClusterHealthCheckInstance)instance).setHealthCheckConfig(healthCheckConfig);
        when(healthCheckConfig.supportSentinelHealthCheck(any(),any())).thenReturn(true);
        checkAction = new SentinelHelloCheckAction(scheduled, instance, executors, config, persistence,metaCache,instanceManager);
        downgradeController = new CurrentDcSentinelHelloAggregationCollector(metaCache, sentinelHelloCollector, clusterName, shardName, checkerConfig);
        downgradeController = Mockito.spy(downgradeController);
        Mockito.when(healthCheckConfig.getSentinelCheckIntervalMilli()).thenReturn(sentinelCheckInterval);
        checkActionController.addCollector(clusterName, shardName, downgradeController);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = sentinelCollectInterval;
        SubscribeCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = sentinelSubTimeout;
        prepareActions();
        prepareMetaCache();
        checkAction.addController(checkActionController);
        checkAction.addListener(checkActionController);
        when(config.isSentinelAutoProcess()).thenReturn(true);
        when(config.shouldSentinelCheck(Mockito.anyString())).thenReturn(true);
        when(persistence.isClusterOnMigration(anyString())).thenReturn(false);
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(jqMasterMeta.getIp(), jqMasterMeta.getPort()))).thenReturn(jqMaster.getRedisCheckInstance());
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(jqSlaveMeta.getIp(), jqSlaveMeta.getPort()))).thenReturn(jqSlave.getRedisCheckInstance());
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(oyMasterMeta.getIp(), oyMasterMeta.getPort()))).thenReturn(oyMaster.getRedisCheckInstance());
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(oySlaveMeta.getIp(), oySlaveMeta.getPort()))).thenReturn(oySlave.getRedisCheckInstance());
    }

    @After
    public void afterSentinelHelloActionDowngradeTest() throws Exception {
        if (null != jqMaster.redisServer) jqMaster.redisServer.stop();
        if (null != jqSlave.redisServer) jqSlave.redisServer.stop();
        if (null != oyMaster.redisServer) oyMaster.redisServer.stop();
        if (null != oySlave.redisServer) oySlave.redisServer.stop();
    }

    @Test
    public void allUpTest() {
        when(checkerConfig.sentinelCheckDowngradeStrategy()).thenReturn("lessThanHalf");
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig());
        //make command do not easily timeout on github
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        // no close connect before collect sentinel hello
        Assert.assertEquals(0, jqMaster.redisServer.getConnected());
        Assert.assertEquals(1, jqSlave.redisServer.getConnected());
        Assert.assertEquals(0, oyMaster.redisServer.getConnected());
        Assert.assertEquals(0, oySlave.redisServer.getConnected());

        verify(sentinelHelloCollector, times(1)).onAction(Mockito.any());
        verify(downgradeController, times(1)).onAction(Mockito.any());
        // not close connect after collect sentinel hello
        Assert.assertEquals(0, jqMaster.redisServer.getConnected());
        Assert.assertEquals(1, jqSlave.redisServer.getConnected());
        Assert.assertEquals(0, oyMaster.redisServer.getConnected());
        Assert.assertEquals(0, oySlave.redisServer.getConnected());

    }

    @Test
    public void slaveErrRespTest() {
        when(checkerConfig.sentinelCheckDowngradeStrategy()).thenReturn("lessThanHalf");
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig());
        Assert.assertFalse(downgradeController.getNeedDowngrade());

        setServerErrResp(false, true, false, false);
        allActionDoTask();

        //unsubscribe if subscribe failed
        Assert.assertEquals(0, oyMaster.redisServer.getConnected());
        Assert.assertEquals(0, oySlave.redisServer.getConnected());
        Assert.assertEquals(0, jqMaster.redisServer.getConnected());
        Assert.assertEquals(0, jqSlave.redisServer.getConnected());
        assertServerCalled(false, true, false, false);
        verify(downgradeController, times(1)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(0)).onAction(Mockito.any());
        Assert.assertTrue(downgradeController.getNeedDowngrade());

        resetCalled();
        allActionDoTask();

        //downgrade master duo to slave sub failed, both subscribe slave
        Assert.assertEquals(1, jqMaster.redisServer.getConnected());
        Assert.assertEquals(0, jqSlave.redisServer.getConnected());
        Assert.assertEquals(0, oyMaster.redisServer.getConnected());
        Assert.assertEquals(0, oySlave.redisServer.getConnected());
        assertServerCalled(true, true, false, false);
        verify(downgradeController, times(3)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(1)).onAction(Mockito.any());
        Assert.assertFalse(downgradeController.getNeedDowngrade());

        setServerErrResp(false, false, false, false);
        resetCalled();
        allActionDoTask();

        //not downgrade
        Assert.assertEquals(0, jqMaster.redisServer.getConnected());
        Assert.assertEquals(1, jqSlave.redisServer.getConnected());
        Assert.assertEquals(0, oyMaster.redisServer.getConnected());
        Assert.assertEquals(0, oySlave.redisServer.getConnected());

        assertServerCalled(false, true, false, false);
        verify(downgradeController, times(4)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(2)).onAction(Mockito.any());
        Assert.assertFalse(downgradeController.getNeedDowngrade());

        resetCalled();
        allActionDoTask();

        //unsubscribe master dc slave when not downgrade
        Assert.assertEquals(0, jqMaster.redisServer.getConnected());
        Assert.assertEquals(1, jqSlave.redisServer.getConnected());
        Assert.assertEquals(0, oyMaster.redisServer.getConnected());
        Assert.assertEquals(0, oySlave.redisServer.getConnected());

    }

    @Test
    public void stopWatchTest() {
        Assert.assertEquals(1, checkActionController.getCollectors().size());
        checkActionController.stopWatch(checkAction);
        verify(downgradeController, times(1)).stopWatch(checkAction);
        Assert.assertEquals(0, checkActionController.getCollectors().size());
    }

    private void prepareActions() throws Exception {
        jqMaster = new CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus("jq", null, true);
        jqMasterMeta.setPort(jqMaster.redisServer.getPort());
        jqSlave = new CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus("jq", null, false);
        jqSlaveMeta.setPort(jqSlave.redisServer.getPort());
        oyMaster = new CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus("oy", null, false);
        oyMasterMeta.setPort(oyMaster.redisServer.getPort());
        oySlave = new CurrentDcSentinelHelloCheckControllerTest.SentinelCheckStatus("oy", null, false);
        oySlaveMeta.setPort(oySlave.redisServer.getPort());
    }

    public void prepareMetaCache() {

        ShardMeta jqShardMeta = new ShardMeta();
        jqShardMeta.setId(shardName);
        jqShardMeta.addRedis(jqMasterMeta);
        jqShardMeta.addRedis(jqSlaveMeta);
        ShardMeta oyShardMeta = new ShardMeta();
        oyShardMeta.setId(shardName);
        oyShardMeta.addRedis(oyMasterMeta);
        oyShardMeta.addRedis(oySlaveMeta);

        ClusterMeta jqClusterMeta = new ClusterMeta();
        jqClusterMeta.setId(clusterName);
        jqClusterMeta.addShard(jqShardMeta);
        ClusterMeta oyClusterMeta = new ClusterMeta();
        oyClusterMeta.setId(clusterName);
        oyClusterMeta.addShard(oyShardMeta);

        DcMeta dc1 = new DcMeta();
        dc1.setId("jq");
        dc1.addCluster(jqClusterMeta);
        DcMeta dc2 = new DcMeta();
        dc2.setId("oy");
        dc2.addCluster(oyClusterMeta);

        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dc1);
        xpipeMeta.addDc(dc2);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    private void resetCalled() {
        jqMaster.called = false;
        jqSlave.called = false;
        oyMaster.called = false;
        oySlave.called = false;
    }

    private void allActionDoTask() {
        sleep(sentinelCheckInterval); // wait for check interval
        allActionDoTaskWithoutWait();
        sleep( sentinelCollectInterval+100); // wait for hello collect
    }

    private void allActionDoTaskWithoutWait() {
        checkAction.new ScheduledHealthCheckTask().run();
    }

    private void setServerHang(boolean jqMasterHang, boolean jqSlaveHang, boolean oyMasterHang, boolean oySlaveHang) {
        jqMaster.serverHang = jqMasterHang;
        jqSlave.serverHang = jqSlaveHang;
        oyMaster.serverHang = oyMasterHang;
        oySlave.serverHang = oySlaveHang;
    }

    private void setServerErrResp(boolean jqMasterErrResp, boolean jqSlaveErrResp, boolean oyMasterErrResp, boolean oySlaveErrResp) {
        jqMaster.errorResp = jqMasterErrResp;
        jqSlave.errorResp = jqSlaveErrResp;
        oyMaster.errorResp = oyMasterErrResp;
        oySlave.errorResp = oySlaveErrResp;
    }

    private void assertServerCalled(boolean jqMasterCalled, boolean jqSlaveCalled, boolean oyMasterCalled, boolean oySlaveCalled) {
        Assert.assertEquals(jqMasterCalled, this.jqMaster.called);
        Assert.assertEquals(jqSlaveCalled, jqSlave.called);
        Assert.assertEquals(oyMasterCalled, oyMaster.called);
        Assert.assertEquals(oyMasterCalled, oySlave.called);
    }

    private String buildSentinelHello(int sentinelNum) {
        StringBuilder sb = new StringBuilder(SentinelHelloCheckActionTest.SUBSCRIBE_HEADER);
        for(int i = 0; i < sentinelNum; i++) {
            sb.append(String.format(SentinelHelloCheckActionTest.SENTINEL_HELLO_TEMPLATE, 5000 + i, jqMaster.redisServer.getPort()));
        }
        return sb.toString();
    }

    private class SentinelCheckStatus {

        public Server redisServer;

        public RedisHealthCheckInstance checkInstance;

        public volatile boolean errorResp = false;

        public volatile boolean serverHang = false;

        public volatile boolean called = false;

        public volatile int sentinelNum = 5;

        public SentinelCheckStatus(String currentDc, String activeDc, boolean isMaster) throws Exception {
            initServer();
            initMeta(currentDc, activeDc, isMaster);
        }

        private void initMeta(String currentDc, String activeDc, boolean isMaster) throws Exception {
            checkInstance = newRandomRedisHealthCheckInstance(currentDc, activeDc, redisServer.getPort(), ClusterType.BI_DIRECTION);
            ((DefaultRedisHealthCheckInstance)checkInstance).setHealthCheckConfig(healthCheckConfig);
            checkInstance.getCheckInfo().isMaster(isMaster);
        }

        public RedisHealthCheckInstance getRedisCheckInstance(){
            return checkInstance;
        }

        private void initServer() throws Exception {
            redisServer = startServer(randomPort(), new IoActionFactory() {
                @Override
                public IoAction createIoAction(Socket socket) {
                    return new AbstractIoAction(socket) {

                        private String readLine = null;

                        @Override
                        protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                            try {
                                called = true;
                                String response = null;

                                if (errorResp) {
                                    response = "-----error response-----";
                                } else if (serverHang) {
                                    sleep(sentinelSubTimeout);
                                } else {
                                    response = buildSentinelHello(sentinelNum);
                                }

                                if (null != response) ous.write(response.getBytes());

                                while (!(errorResp || serverHang)) {
                                    ous.write(String.format(SentinelHelloCheckActionTest.SENTINEL_HELLO_TEMPLATE, 5001, jqMaster.redisServer.getPort()).getBytes());
//                                    ous.write(buildSentinelHello().getBytes());
                                    sleep(10);
                                }
                            } catch (Exception e) {
                                logger.error("[doWrite] " + e.getMessage());
                            }
                        }

                        @Override
                        protected Object doRead(InputStream ins) throws IOException {
                            readLine = AbstractIoAction.readLine(ins);
                            logger.info("[doRead]{}", readLine == null ? null : readLine.trim());
                            return readLine;
                        }
                    };
                }
            });
        }

    }

}
