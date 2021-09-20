package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SentinelHelloActionDowngradeTest extends AbstractCheckerTest {

    private SentinelCheckStatus activeDcMaster;
    private SentinelCheckStatus activeDcSlave;
    private SentinelCheckStatus backupDcSlave1;
    private SentinelCheckStatus backupDcSlave2;

    private RedisMeta activeDcMasterMeta = new RedisMeta().setIp("activeDcMasterMeta");
    private RedisMeta activeDcSlaveMeta = new RedisMeta().setIp("activeDcSlaveMeta");
    private RedisMeta backupDcSlave1Meta = new RedisMeta().setIp("backupDcSlave1Meta");
    private RedisMeta backupDcSlave2Meta = new RedisMeta().setIp("backupDcSlave2Meta");

    @Mock
    protected CheckerDbConfig config;

    @Mock
    protected Persistence persistence;

    private int sentinelCollectInterval = 600;

    private int sentinelCheckInterval = 800;

    private int sentinelSubTimeout = 200;

    @InjectMocks
    private SentinelCheckDowngradeManager checkActionController;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Mock
    private DefaultSentinelHelloCollector sentinelHelloCollector;

    @Mock
    private HealthCheckInstanceManager instanceManager;

    private SentinelHelloCheckAction checkAction;

    private ClusterHealthCheckInstance instance;

    private SentinelCheckDowngradeCollectorController downgradeController;

    private static final String activeDc = "dc1";

    private static final String clusterName = "cluster";

    private static final String shardName = "shard";

    @Before
    public void beforeSentinelHelloActionDowngradeTest() throws Exception {
        instance = newRandomClusterHealthCheckInstance(activeDc, ClusterType.ONE_WAY);
        ((DefaultClusterHealthCheckInstance)instance).setHealthCheckConfig(healthCheckConfig);
        checkAction = new SentinelHelloCheckAction(scheduled, instance, executors, config, persistence,metaCache,instanceManager);
        downgradeController = new SentinelCheckDowngradeCollectorController(metaCache, sentinelHelloCollector, clusterName, shardName);
        downgradeController = Mockito.spy(downgradeController);
        Mockito.when(healthCheckConfig.getSentinelCheckIntervalMilli()).thenReturn(sentinelCheckInterval);
        checkActionController.addCheckCollectorController(clusterName, shardName, downgradeController);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = sentinelCollectInterval;
        SubscribeCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = sentinelSubTimeout;
        prepareActions();
        prepareMetaCache();
        checkAction.addController(checkActionController);
        checkAction.addListener(checkActionController);
        when(config.isSentinelAutoProcess()).thenReturn(true);
        when(config.shouldSentinelCheck(Mockito.anyString())).thenReturn(true);
        when(persistence.isClusterOnMigration(anyString())).thenReturn(false);
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(activeDcMasterMeta.getIp(),activeDcMasterMeta.getPort()))).thenReturn(activeDcMaster.getRedisCheckInstance());
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(activeDcSlaveMeta.getIp(),activeDcSlaveMeta.getPort()))).thenReturn(activeDcSlave.getRedisCheckInstance());
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(backupDcSlave1Meta.getIp(),backupDcSlave1Meta.getPort()))).thenReturn(backupDcSlave1.getRedisCheckInstance());
        when(instanceManager.findRedisHealthCheckInstance(new HostPort(backupDcSlave2Meta.getIp(),backupDcSlave2Meta.getPort()))).thenReturn(backupDcSlave2.getRedisCheckInstance());
    }

    @After
    public void afterSentinelHelloActionDowngradeTest() throws Exception {
        if (null != activeDcMaster.redisServer) activeDcMaster.redisServer.stop();
        if (null != activeDcSlave.redisServer) activeDcSlave.redisServer.stop();
        if (null != backupDcSlave1.redisServer) backupDcSlave1.redisServer.stop();
        if (null != backupDcSlave2.redisServer) backupDcSlave2.redisServer.stop();
    }

    @Test
    public void allUpTest() {
        allActionDoTask();

        assertServerCalled(false, false, true, true);
        // no close connect before collect sentinel hello
        Assert.assertEquals(1, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(1, backupDcSlave2.redisServer.getConnected());

        verify(sentinelHelloCollector, times(1)).onAction(Mockito.any());
        verify(downgradeController, times(2)).onAction(Mockito.any());
        // not close connect after collect sentinel hello
        Assert.assertEquals(1, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(1, backupDcSlave2.redisServer.getConnected());

        // break one slave in backup dc
        backupDcSlave1.errorResp = true;
        resetCalled();
        allActionDoTask();
        // do not resubscribe if successfully subscribed
        assertServerCalled(false, false, false, false);
        verify(sentinelHelloCollector, times(2)).onAction(Mockito.any());
        verify(downgradeController, times(4)).onAction(Mockito.any());
    }

    @Test
    public void backupErrRespTest() {
        setServerErrResp(false, false, true, true);
        allActionDoTask();

        //unsubscribe if subscribe failed
        Assert.assertEquals(0, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(0, backupDcSlave2.redisServer.getConnected());
        Assert.assertEquals(0, activeDcMaster.redisServer.getConnected());
        Assert.assertEquals(0, activeDcSlave.redisServer.getConnected());
        assertServerCalled(false, false, true, true);
        verify(sentinelHelloCollector, times(0)).onAction(Mockito.any());
        verify(downgradeController, times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        //downgrade active dc slave, but unsubscribe dr slaves duo to sub failed
        Assert.assertEquals(0, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(0, backupDcSlave2.redisServer.getConnected());
        Assert.assertEquals(0, activeDcMaster.redisServer.getConnected());
        Assert.assertEquals(1, activeDcSlave.redisServer.getConnected());
        assertServerCalled(false, true, false, false);
        verify(downgradeController, times(3)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(1)).onAction(Mockito.any());


        setServerErrResp(false, false, false, false);
        resetCalled();
        allActionDoTask();

        //not downgrade
        Assert.assertEquals(1, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(1, backupDcSlave2.redisServer.getConnected());
        Assert.assertEquals(0, activeDcMaster.redisServer.getConnected());
        Assert.assertEquals(0, activeDcSlave.redisServer.getConnected());

        assertServerCalled(false, false, true, true);
        verify(downgradeController, times(5)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        //unsubscribe master dc slave when not downgrade
        Assert.assertEquals(1, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(1, backupDcSlave2.redisServer.getConnected());
        Assert.assertEquals(0, activeDcMaster.redisServer.getConnected());
        Assert.assertEquals(0, activeDcSlave.redisServer.getConnected());
    }

    @Test
    public void backupCommandTimeoutTest() {
        setServerHang(false, false, true, true);
        allActionDoTask();

        assertServerCalled(false, false, true, true);
        // close connect immediately after time out
        Assert.assertEquals(0, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(0, backupDcSlave2.redisServer.getConnected());
        verify(sentinelHelloCollector, times(0)).onAction(Mockito.any());
        verify(downgradeController, times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        verify(downgradeController, times(3)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(1)).onAction(Mockito.any());

        setServerHang(false, false, false, false);
        resetCalled();
        allActionDoTask();


        assertServerCalled(false, false, true, true);
        verify(downgradeController, times(5)).onAction(Mockito.any());
        verify(sentinelHelloCollector, times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, false, false, false);
    }

    @Test
    public void backupLoseConnectTest() throws Exception {
        backupDcSlave1.redisServer.stop();
        backupDcSlave2.redisServer.stop();
        backupDcSlave1.redisServer = null;
        backupDcSlave2.redisServer = null;

        allActionDoTask();

        assertServerCalled(false, false, false, false);
        verify(sentinelHelloCollector, times(0)).onAction(Mockito.any());
        verify(downgradeController, times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        verify(sentinelHelloCollector, times(1)).onAction(Mockito.any());
        verify(downgradeController, times(3)).onAction(Mockito.any());
    }

    @Test
    public void stopWatchTest() {
        Assert.assertEquals(1, checkActionController.getAllCheckCollectorControllers().size());
        checkActionController.stopWatch(checkAction);
        verify(downgradeController, times(1)).stopWatch(checkAction);
        Assert.assertEquals(0, checkActionController.getAllCheckCollectorControllers().size());
    }

    private void prepareActions() throws Exception {
        activeDcMaster = new SentinelCheckStatus("dc1", activeDc, true);
        activeDcMasterMeta.setPort(activeDcMaster.redisServer.getPort());
        activeDcSlave = new SentinelCheckStatus("dc1", activeDc, false);
        activeDcSlaveMeta.setPort(activeDcSlave.redisServer.getPort());
        backupDcSlave1 = new SentinelCheckStatus("dc2", activeDc, false);
        backupDcSlave1Meta.setPort(backupDcSlave1.redisServer.getPort());
        backupDcSlave2 = new SentinelCheckStatus("dc2", activeDc, false);
        backupDcSlave2Meta.setPort(backupDcSlave2.redisServer.getPort());
    }

    public void prepareMetaCache() {

        ShardMeta activeDcShardMeta = new ShardMeta();
        activeDcShardMeta.setId(shardName);
        activeDcShardMeta.addRedis(activeDcMasterMeta);
        activeDcShardMeta.addRedis(activeDcSlaveMeta);
        ShardMeta backupDcShardMeta = new ShardMeta();
        backupDcShardMeta.setId(shardName);
        backupDcShardMeta.addRedis(backupDcSlave1Meta);
        backupDcShardMeta.addRedis(backupDcSlave2Meta);

        ClusterMeta activeDcClusterMeta = new ClusterMeta();
        activeDcClusterMeta.setId(clusterName);
        activeDcClusterMeta.setActiveDc("dc1");
        activeDcClusterMeta.addShard(activeDcShardMeta);
        ClusterMeta backupDcClusterMeta = new ClusterMeta();
        backupDcClusterMeta.setId(clusterName);
        backupDcClusterMeta.setActiveDc("dc1");
        backupDcClusterMeta.addShard(backupDcShardMeta);

        DcMeta dc1 = new DcMeta();
        dc1.setId("dc1");
        dc1.addCluster(activeDcClusterMeta);
        DcMeta dc2 = new DcMeta();
        dc2.setId("dc2");
        dc2.addCluster(backupDcClusterMeta);

        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dc1);
        xpipeMeta.addDc(dc2);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    private void resetCalled() {
        activeDcMaster.called = false;
        activeDcSlave.called = false;
        backupDcSlave1.called = false;
        backupDcSlave2.called = false;
    }

    private void allActionDoTask() {
        sleep(sentinelCheckInterval); // wait for check interval
        allActionDoTaskWithoutWait();
        sleep( sentinelCollectInterval+100); // wait for hello collect
    }

    private void allActionDoTaskWithoutWait() {
        checkAction.new ScheduledHealthCheckTask().run();
    }

    private void setServerHang(boolean activeDcMasterHang, boolean activeDcSlaveHang, boolean backupDcSlave1Hang, boolean backupDcSlave2Hang) {
        activeDcMaster.serverHang = activeDcMasterHang;
        activeDcSlave.serverHang = activeDcSlaveHang;
        backupDcSlave1.serverHang = backupDcSlave1Hang;
        backupDcSlave2.serverHang = backupDcSlave2Hang;
    }

    private void setServerErrResp(boolean activeDcMasterErrResp, boolean activeDcSlaveErrResp, boolean backupDcSlave1ErrResp, boolean backupDcSlave2ErrResp) {
        activeDcMaster.errorResp = activeDcMasterErrResp;
        activeDcSlave.errorResp = activeDcSlaveErrResp;
        backupDcSlave1.errorResp = backupDcSlave1ErrResp;
        backupDcSlave2.errorResp = backupDcSlave2ErrResp;
    }

    private void assertServerCalled(boolean activeDcMasterCalled, boolean activeDcSlaveCalled, boolean backupDcSlave1Called, boolean backDcSlave2Called) {
        Assert.assertEquals(activeDcMasterCalled, activeDcMaster.called);
        Assert.assertEquals(activeDcSlaveCalled, activeDcSlave.called);
        Assert.assertEquals(backupDcSlave1Called, backupDcSlave1.called);
        Assert.assertEquals(backDcSlave2Called, backupDcSlave2.called);
    }

    private String buildSentinelHello() {
        StringBuilder sb = new StringBuilder(SentinelHelloCheckActionTest.SUBSCRIBE_HEADER);
        for(int i = 0; i < 5; i++) {
            sb.append(String.format(SentinelHelloCheckActionTest.SENTINEL_HELLO_TEMPLATE, 5000 + i,activeDcMaster.redisServer.getPort()));
        }
        return sb.toString();
    }

    private class SentinelCheckStatus {

        public Server redisServer;

        public RedisHealthCheckInstance checkInstance;

        public volatile boolean errorResp = false;

        public volatile boolean serverHang = false;

        public volatile boolean called = false;

        public SentinelCheckStatus(String currentDc, String activeDc, boolean isMaster) throws Exception {
            initServer();
            initMeta(currentDc, activeDc, isMaster);
        }

        private void initMeta(String currentDc, String activeDc, boolean isMaster) throws Exception {
            checkInstance = newRandomRedisHealthCheckInstance(currentDc, activeDc, redisServer.getPort());
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
                                    response = buildSentinelHello();
                                }

                                if (null != response) ous.write(response.getBytes());

                                while (!(errorResp || serverHang)) {
                                    ous.write(String.format(SentinelHelloCheckActionTest.SENTINEL_HELLO_TEMPLATE, 5001, activeDcMaster.redisServer.getPort()).getBytes());
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
