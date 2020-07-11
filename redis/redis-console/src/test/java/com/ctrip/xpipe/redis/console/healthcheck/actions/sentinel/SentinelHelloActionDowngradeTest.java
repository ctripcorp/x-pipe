package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.AbstractHealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.core.entity.*;
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
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SentinelHelloActionDowngradeTest extends AbstractConsoleTest {

    private SentinelCheckStatus activeDcMaster;
    private SentinelCheckStatus activeDcSlave;
    private SentinelCheckStatus backupDcSlave1;
    private SentinelCheckStatus backupDcSlave2;

    @Mock
    protected ConsoleDbConfig config;

    @Mock
    protected ClusterService clusterService;

    private int sentinelCollectInterval = 300;

    private int sentinelCheckInterval = 2000;

    private int sentinelSubTimeout = 200;

    @InjectMocks
    private SentinelCheckDowngradeManager checkActionController;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Mock
    private DefaultSentinelHelloCollector sentinelHelloCollector;

    private SentinelCheckDowngradeCollectorController downgradeController;

    private static final String clusterName = "cluster";

    private static final String shardName = "shard";

    @Before
    public void beforeSentinelHelloActionDowngradeTest() throws Exception {
        downgradeController = new SentinelCheckDowngradeCollectorController(metaCache, sentinelHelloCollector, clusterName, shardName);
        downgradeController = Mockito.spy(downgradeController);
        Mockito.when(healthCheckConfig.getSentinelCheckIntervalMilli()).thenReturn(sentinelCheckInterval);
        checkActionController.addCheckCollectorController(clusterName, shardName, downgradeController);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = sentinelCollectInterval;
        SubscribeCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = sentinelSubTimeout;
        prepareActions();
        prepareMetaCache();

        when(config.isSentinelAutoProcess()).thenReturn(true);
        when(config.shouldSentinelCheck(Mockito.anyString(), Mockito.anyBoolean())).thenReturn(true);
        when(clusterService.find(anyString())).thenReturn(new ClusterTbl().setStatus(ClusterStatus.Normal.toString()));
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
        setSentinelCollectInterval(1000);
        sleep(sentinelCheckInterval);
        allActionDoTaskWithoutWait();

        sleep(300);
        assertServerCalled(false, false, true, true);
        // no close connect before collect sentinel hello
        Assert.assertEquals(1, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(1, backupDcSlave2.redisServer.getConnected());
        sleep(1000);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());
        // close connect after collect sentinel hello
        Assert.assertEquals(0, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(0, backupDcSlave2.redisServer.getConnected());

        // break one slave in backup dc
        backupDcSlave1.errorResp = true;
        resetCalled();
        allActionDoTask();

        assertServerCalled(false, false, true, true);
        Mockito.verify(sentinelHelloCollector, Mockito.times(2)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(4)).onAction(Mockito.any());
    }

    @Test
    public void backupErrRespTest() {
        setServerErrResp(false, false, true, true);

        allActionDoTask();

        assertServerCalled(false, false, true, true);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(3)).onAction(Mockito.any());

        setServerErrResp(false, false, false, false);
        resetCalled();
        allActionDoTask();

        assertServerCalled(false, false, true, true);
        Mockito.verify(sentinelHelloCollector, Mockito.times(2)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(5)).onAction(Mockito.any());
    }

    @Test
    public void backupCommandTimeoutTest() {
        setServerHang(false, false, true, true);
        allActionDoTask();

        assertServerCalled(false, false, true, true);
        // close connect immediately after time out
        Assert.assertEquals(0, backupDcSlave1.redisServer.getConnected());
        Assert.assertEquals(0, backupDcSlave2.redisServer.getConnected());
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(3)).onAction(Mockito.any());

        setServerHang(false, false, false, false);
        resetCalled();
        allActionDoTask();


        assertServerCalled(false, false, true, true);
        Mockito.verify(sentinelHelloCollector, Mockito.times(2)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(5)).onAction(Mockito.any());
    }

    @Test
    public void backupLoseConnectTest() throws Exception {
        backupDcSlave1.redisServer.stop();
        backupDcSlave2.redisServer.stop();
        backupDcSlave1.redisServer = null;
        backupDcSlave2.redisServer = null;

        allActionDoTask();

        assertServerCalled(false, false, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(3)).onAction(Mockito.any());
    }

    @Test
    public void backupConnectTimeoutTest() throws Exception {
        // set unreachable redis server
        ((DefaultRedisHealthCheckInstance)backupDcSlave1.checkInstance).setSession(
                new RedisSession(new DefaultEndPoint("10.0.127.1", 6379), scheduled, getXpipeNettyClientKeyedObjectPool()));
        ((DefaultRedisHealthCheckInstance)backupDcSlave2.checkInstance).setSession(
                new RedisSession(new DefaultEndPoint("10.0.127.1", 6379), scheduled, getXpipeNettyClientKeyedObjectPool()));

        allActionDoTask();
        assertServerCalled(false, false, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, true, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(3)).onAction(Mockito.any());
    }


    @Test
    public void downgradeTimeoutTest() {
        setServerErrResp(false, false, true, true);

        allActionDoTask();

        assertServerCalled(false, false, true, true);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        sleep(3 * sentinelCheckInterval);

        resetCalled();
        allActionDoTask();

        assertServerCalled(false, false, true, true);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(4)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();
        // should not always timeout, so do downgrade
        assertServerCalled(false, true, false, false);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(5)).onAction(Mockito.any());
    }

    private void prepareActions() throws Exception {
        activeDcMaster = new SentinelCheckStatus("dc1", "dc1", true);
        activeDcSlave = new SentinelCheckStatus("dc1", "dc1", false);
        backupDcSlave1 = new SentinelCheckStatus("dc2", "dc1", false);
        backupDcSlave2 = new SentinelCheckStatus("dc2", "dc1", false);
    }

    public void prepareMetaCache() {
        RedisMeta activeDcMaster = new RedisMeta();
        RedisMeta activeDcSlave = new RedisMeta();
        RedisMeta backupDcSlave1 = new RedisMeta();
        RedisMeta backupDcSlave2 = new RedisMeta();

        ShardMeta activeDcShardMeta = new ShardMeta();
        activeDcShardMeta.setId(shardName);
        activeDcShardMeta.addRedis(activeDcMaster);
        activeDcShardMeta.addRedis(activeDcSlave);
        ShardMeta backupDcShardMeta = new ShardMeta();
        backupDcShardMeta.setId(shardName);
        backupDcShardMeta.addRedis(backupDcSlave1);
        backupDcShardMeta.addRedis(backupDcSlave2);

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
        sleep(sentinelCheckInterval);
        allActionDoTaskWithoutWait();
        sleep(sentinelCollectInterval * 2);
    }

    private void allActionDoTaskWithoutWait() {
        activeDcMaster.checkTask.run();
        activeDcSlave.checkTask.run();
        backupDcSlave1.checkTask.run();
        backupDcSlave2.checkTask.run();
    }

    private void setSentinelCollectInterval(int interval) {
        this.sentinelCollectInterval = interval;
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = sentinelCollectInterval;
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
            sb.append(String.format(SentinelHelloCheckActionTest.SENTINEL_HELLO_TEMPLATE, 5000 + i));
        }
        return sb.toString();
    }

    private class SentinelCheckStatus {

        public Server redisServer;

        public RedisHealthCheckInstance checkInstance;

        public SentinelHelloCheckAction checkAction;

        public AbstractHealthCheckAction.ScheduledHealthCheckTask checkTask;

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
            checkInstance.getRedisInstanceInfo().isMaster(isMaster);
            checkAction = new SentinelHelloCheckAction(scheduled, checkInstance, executors, config, clusterService);
            checkAction.addController(checkActionController);
            checkAction.addListener(checkActionController);
            checkTask = checkAction.new ScheduledHealthCheckTask();
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

                                while (true) {
                                    ous.write(String.format(SentinelHelloCheckActionTest.SENTINEL_HELLO_TEMPLATE, 5001).getBytes());
                                    sleep(10);
                                }
                            } catch (Exception e) {
                                logger.error("[doWrite] " + e.getMessage());
                            }
                        }

                        @Override
                        protected Object doRead(InputStream ins) throws IOException {
                            readLine = readLine(ins);
                            logger.info("[doRead]{}", readLine == null ? null : readLine.trim());
                            return readLine;
                        }
                    };
                }
            });
        }

    }

}
