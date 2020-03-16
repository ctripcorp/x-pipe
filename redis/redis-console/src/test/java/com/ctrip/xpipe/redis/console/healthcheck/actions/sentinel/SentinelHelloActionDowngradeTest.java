package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
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

import java.util.concurrent.Callable;

@RunWith(MockitoJUnitRunner.class)
public class SentinelHelloActionDowngradeTest extends SentinelHelloCheckActionTest {

    private Server activeDcMasterServer;
    private Server activeDcSlaveServer;
    private Server backupDcSlave1Server;
    private Server backupDcSlave2Server;

    private RedisHealthCheckInstance masterInstance;
    private RedisHealthCheckInstance activeDcSlaveInstance;
    private RedisHealthCheckInstance backupDcSlave1Instance;
    private RedisHealthCheckInstance backupDcSlave2Instance;

    private SentinelHelloCheckAction activeDcMasterAction;
    private SentinelHelloCheckAction activeDcSlaveAction;
    private SentinelHelloCheckAction backupDcSlave1Action;
    private SentinelHelloCheckAction backupDcSlave2Action;

    private boolean activeDcMasterAvailable = true;
    private boolean activeDcSlaveAvailable = true;
    private boolean backupDcSlave1Available = true;
    private boolean backupDcSlave2Available = true;

    private volatile boolean activeDcMasterServerCalled = false;
    private volatile boolean activeDcSlaveServerCalled = false;
    private volatile boolean backupDcSlave1ServerCalled = false;
    private volatile boolean backupDcSlave2ServerCalled = false;

    private int sentinelCollectInterval = 100;

    private int sentinelCheckInterval = 1200;

    @InjectMocks
    private SentinelHelloCheckActionController checkActionController;

    @Mock
    private SentinelCheckControllerManager checkControllerManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckConfig healthCheckConfig;

    @Mock
    private DefaultSentinelHelloCollector sentinelHelloCollector;

    private SentinelCheckDowngradeController downgradeController;

    private static final String clusterName = "cluster";

    private static final String shardName = "shard";

    @Before
    public void beforeSentinelHelloActionDowngradeTest() throws Exception {
        downgradeController = new SentinelCheckDowngradeController(metaCache, sentinelHelloCollector, clusterName, shardName);
        downgradeController = Mockito.spy(downgradeController);
        Mockito.when(checkControllerManager.getCheckController(clusterName, shardName)).thenReturn(downgradeController);
        Mockito.when(healthCheckConfig.getSentinelCheckIntervalMilli()).thenReturn(sentinelCheckInterval);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = sentinelCollectInterval;
        prepareActions();
        prepareMetaCache();
    }

    @After
    public void afterSentinelHelloActionDowngradeTest() throws Exception {
        activeDcMasterServer.stop();
        activeDcSlaveServer.stop();
        backupDcSlave1Server.stop();
        backupDcSlave2Server.stop();
    }

    @Test
    public void allUpTest() {
        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertFalse(activeDcSlaveServerCalled);
        Assert.assertTrue(backupDcSlave1ServerCalled);
        Assert.assertTrue(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        backupDcSlave1Available = false;
        resetCalled();
        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertFalse(activeDcSlaveServerCalled);
        Assert.assertTrue(backupDcSlave1ServerCalled);
        Assert.assertTrue(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(2)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(4)).onAction(Mockito.any());
    }

    @Test
    public void backupDownTest() {
        backupDcSlave1Available = false;
        backupDcSlave2Available = false;
        activeDcSlaveAvailable = false;

        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertFalse(activeDcSlaveServerCalled);
        Assert.assertTrue(backupDcSlave1ServerCalled);
        Assert.assertTrue(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        resetCalled();
        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertTrue(activeDcSlaveServerCalled);
        Assert.assertFalse(backupDcSlave1ServerCalled);
        Assert.assertFalse(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(3)).onAction(Mockito.any());

        backupDcSlave1Available = true;
        backupDcSlave2Available = true;
        activeDcSlaveAvailable = true;
        resetCalled();
        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertFalse(activeDcSlaveServerCalled);
        Assert.assertTrue(backupDcSlave1ServerCalled);
        Assert.assertTrue(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(2)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(5)).onAction(Mockito.any());
    }

    @Test
    public void downgradeTimeoutTest() {
        backupDcSlave1Available = false;
        backupDcSlave2Available = false;

        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertFalse(activeDcSlaveServerCalled);
        Assert.assertTrue(backupDcSlave1ServerCalled);
        Assert.assertTrue(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(0)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(2)).onAction(Mockito.any());

        sleep(3 * sentinelCheckInterval);

        backupDcSlave1Available = true;
        backupDcSlave2Available = true;
        resetCalled();
        allActionDoTask();

        Assert.assertFalse(activeDcMasterServerCalled);
        Assert.assertFalse(activeDcSlaveServerCalled);
        Assert.assertTrue(backupDcSlave1ServerCalled);
        Assert.assertTrue(backupDcSlave2ServerCalled);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
        Mockito.verify(downgradeController, Mockito.times(4)).onAction(Mockito.any());
    }

    private void prepareActions() throws Exception {
        activeDcMasterServer = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() {
                activeDcMasterServerCalled = true;
                if (activeDcMasterAvailable) {
                    return buildSentinelHello();
                } else {
                    return "-----error response-----";
                }
            }
        });
        masterInstance = newRandomRedisHealthCheckInstance("dc1", "dc1", activeDcMasterServer.getPort());
        ((DefaultRedisHealthCheckInstance)masterInstance).setHealthCheckConfig(healthCheckConfig);
        masterInstance.getRedisInstanceInfo().isMaster(true);
        activeDcMasterAction = new SentinelHelloCheckAction(scheduled, masterInstance, executors, config, clusterService);
        activeDcMasterAction.addController(checkActionController);
        activeDcMasterAction.addListener(checkActionController);

        activeDcSlaveServer = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() {
                activeDcSlaveServerCalled = true;
                if (activeDcSlaveAvailable) {
                    return buildSentinelHello();
                } else {
                    return "-----error response-----";
                }
            }
        });
        activeDcSlaveInstance = newRandomRedisHealthCheckInstance("dc1", "dc1", activeDcSlaveServer.getPort());
        ((DefaultRedisHealthCheckInstance)activeDcSlaveInstance).setHealthCheckConfig(healthCheckConfig);
        activeDcSlaveInstance.getRedisInstanceInfo().isMaster(false);
        activeDcSlaveAction = new SentinelHelloCheckAction(scheduled, activeDcSlaveInstance, executors, config, clusterService);
        activeDcSlaveAction.addController(checkActionController);
        activeDcSlaveAction.addListener(checkActionController);

        backupDcSlave1Server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() {
                backupDcSlave1ServerCalled = true;
                if (backupDcSlave1Available) {
                    return buildSentinelHello();
                } else {
                    return "-----error response-----";
                }
            }
        });
        backupDcSlave1Instance = newRandomRedisHealthCheckInstance("dc2", "dc1", backupDcSlave1Server.getPort());
        ((DefaultRedisHealthCheckInstance)backupDcSlave1Instance).setHealthCheckConfig(healthCheckConfig);
        backupDcSlave1Instance.getRedisInstanceInfo().isMaster(false);
        backupDcSlave1Action = new SentinelHelloCheckAction(scheduled, backupDcSlave1Instance, executors, config, clusterService);
        backupDcSlave1Action.addController(checkActionController);
        backupDcSlave1Action.addListener(checkActionController);

        backupDcSlave2Server = startServerWithFlexibleResult(new Callable<String>() {
            @Override
            public String call() {
                backupDcSlave2ServerCalled = true;
                if (backupDcSlave2Available) {
                    return buildSentinelHello();
                } else {
                    return "-----error response-----";
                }
            }
        });
        backupDcSlave2Instance = newRandomRedisHealthCheckInstance("dc2", "dc1", backupDcSlave2Server.getPort());
        ((DefaultRedisHealthCheckInstance)backupDcSlave2Instance).setHealthCheckConfig(healthCheckConfig);
        backupDcSlave2Instance.getRedisInstanceInfo().isMaster(false);
        backupDcSlave2Action = new SentinelHelloCheckAction(scheduled, backupDcSlave2Instance, executors, config, clusterService);
        backupDcSlave2Action.addController(checkActionController);
        backupDcSlave2Action.addListener(checkActionController);
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
        activeDcMasterServerCalled = false;
        activeDcSlaveServerCalled = false;
        backupDcSlave1ServerCalled = false;
        backupDcSlave2ServerCalled = false;
    }

    private void allActionDoTask() {
        sleep(sentinelCheckInterval);
        activeDcMasterAction.doTask();
        activeDcSlaveAction.doTask();
        backupDcSlave1Action.doTask();
        backupDcSlave2Action.doTask();
        sleep(sentinelCollectInterval * 2);
    }

    private String buildSentinelHello() {
        StringBuilder sb = new StringBuilder(SUBSCRIBE_HEADER);
        for(int i = 0; i < 5; i++) {
            sb.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i));
        }
        return sb.toString();
    }

}
