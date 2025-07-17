package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.channel.ConnectTimeoutException;
import org.junit.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.HELLO_CHANNEL;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloCheckActionTest extends AbstractCheckerTest {

    public static final String SUBSCRIBE_HEADER = "*3\r\n" +
            "$9\r\n" + "subscribe\r\n" +
            "$18\r\n" + "__sentinel__:hello\r\n" +
            ":1\r\n";

    public static final String SENTINEL_HELLO_TEMPLATE = "*3\r\n" +
            "$7\r\n" + "message\r\n" +
            "$18\r\n" + "__sentinel__:hello\r\n" +
            "$84\r\n" +
            "127.0.0.1,%d,d156c06308a5e5c6edba1f8786b32e22cfceafcc,8410,shard,127.0.0.1,%d,0\r\n";

    private SentinelHelloCheckAction action;

    private static final String clusterName = "cluster";

    private static final String shardName1 = "shard1";
    private static final String shardName2 = "shard2";
    private static final String shardName3 = "shard3";

    private static final String ACTIVE_DC_SHARD1_MASTER = "activeDcShard1Master";
    private static final String ACTIVE_DC_SHARD1_SLAVE = "activeDcShard1Slave";
    private static final String BACKUP_DC_SHARD1_SLAVE1 = "backUpDcShard1Slave1";
    private static final String BACKUP_DC_SHARD1_SLAVE2 = "backUpDcShard1Slave2";


    private static final String ACTIVE_DC_SHARD2_MASTER = "activeDcShard2Master";
    private static final String ACTIVE_DC_SHARD2_SLAVE = "activeDcShard2Slave";
    private static final String BACKUP_DC_SHARD2_SLAVE1 = "backUpDcShard2Slave1";
    private static final String BACKUP_DC_SHARD2_SLAVE2 = "backUpDcShard2Slave2";


    private static final String ACTIVE_DC_SHARD3_MASTER = "activeDcShard3Master";
    private static final String ACTIVE_DC_SHARD3_SLAVE = "activeDcShard3Slave";

    private List<String> redisNames = new ArrayList<>();
    private Map<String, Server> servers = new HashMap<>();
    private Map<String, RedisMeta> redisMetas = new HashMap<>();
    private Map<String, RedisHealthCheckInstance> redisHealthCheckInstances = new HashMap<>();
    private Map<String, Supplier<String>> serverResults = new HashMap<>();

    @Mock
    private MetaCache metaCache;

    @Mock
    protected CheckerDbConfig config;

    @Mock
    private PersistenceCache persistenceCache;

    @Mock
    private HealthCheckInstanceManager instanceManager;

    @Mock
    private HealthCheckActionController healthCheckActionController;

    private ClusterHealthCheckInstance instance;

    @SuppressWarnings("unchecked")
    @Before
    public void beforeSentinelHelloCheckActionTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        redisNames = Lists.newArrayList(ACTIVE_DC_SHARD1_MASTER, ACTIVE_DC_SHARD1_SLAVE, BACKUP_DC_SHARD1_SLAVE1, BACKUP_DC_SHARD1_SLAVE2,
                ACTIVE_DC_SHARD2_MASTER, ACTIVE_DC_SHARD2_SLAVE, BACKUP_DC_SHARD2_SLAVE1, BACKUP_DC_SHARD2_SLAVE2, ACTIVE_DC_SHARD3_MASTER, ACTIVE_DC_SHARD3_SLAVE);

        for (String redisIp : redisNames) {
            serverResults.put(redisIp, new Supplier<String>() {
                @Override
                public String get() {
                    return "";
                }
            });
            servers.put(redisIp, startServerWithFlexibleResult(new Callable<String>() {
                @Override
                public String call() {
                    if (serverResults.get(redisIp) != null) {
                        return serverResults.get(redisIp).get();
                    } else {
                        return "";
                    }
                }
            }));
            redisMetas.put(redisIp, newRandomFakeRedisMeta().setPort(servers.get(redisIp).getPort()));

            redisHealthCheckInstances.put(redisIp, newRandomRedisHealthCheckInstance("dc1", servers.get(redisIp).getPort()));

            when(instanceManager.findRedisHealthCheckInstance(new HostPort(LOCAL_HOST, servers.get(redisIp).getPort()))).thenReturn(redisHealthCheckInstances.get(redisIp));

            if (redisIp.contains("activeDc"))
                when(healthCheckActionController.shouldCheck(redisHealthCheckInstances.get(redisIp))).thenReturn(false);
            else
                when(healthCheckActionController.shouldCheck(redisHealthCheckInstances.get(redisIp))).thenReturn(true);
        }

        instance = newRandomClusterHealthCheckInstance("dc1", ClusterType.ONE_WAY);
        when(config.isSentinelAutoProcess()).thenReturn(true);
        when(config.shouldSentinelCheck(anyString())).thenReturn(true);

        prepareMetaCache();
        when(persistenceCache.isClusterOnMigration(anyString())).thenReturn(false);
        action = new SentinelHelloCheckAction(scheduled, instance, executors, config, persistenceCache, metaCache, instanceManager);
        action.addController(healthCheckActionController);
    }

    @After
    public void afterSentinelHelloCheckActionTest() throws Exception {
        for (Server server : servers.values())
            try {
                server.stop();
            } catch (Exception e) {
                // ignore
            }
    }

    @Test
    public void testDoScheduledTaskWithProcessOff() {
        action = spy(action);
        when(config.isSentinelAutoProcess()).thenReturn(false);
        if (action.shouldCheck(instance))
            action.doTask();
        verify(action, never()).processSentinelHellos();
    }

    @Test
    public void testDoScheduledTaskWithInSentinelCheckWhitelist() {
        action = spy(action);
        when(config.isSentinelAutoProcess()).thenReturn(true);
        when(config.shouldSentinelCheck(Mockito.anyString())).thenReturn(false);
        if (action.shouldCheck(instance))
            action.doTask();
        verify(action, never()).processSentinelHellos();
    }

    @Test
    public void testDoScheduleTask() {
        action = spy(action);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = 10;
        action.doTask();
        sleep(30);
        verify(action, times(1)).processSentinelHellos();
    }

    @Test
    public void testDoScheduleTaskInterval() {
        action = spy(action);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = 10;
        ScheduledFuture f = scheduled.scheduleWithFixedDelay(action.new ScheduledHealthCheckTask(), 0, 200, TimeUnit.MILLISECONDS);

        sleep(1000);
        f.cancel(false);
        verify(action, atLeast(3)).processSentinelHellos();
    }

    @Test
    @Ignore
    public void testDoScheduleTaskWithSentinelHelloSuccess() throws Exception {
        StringBuilder shard1Builder = new StringBuilder(SUBSCRIBE_HEADER);
        for (int i = 0; i < 5; i++) {
            shard1Builder.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i, servers.get(ACTIVE_DC_SHARD1_MASTER).getPort()));
        }
        for (String redisIp : serverResults.keySet()) {
            if (redisIp.contains("Shard1"))
                serverResults.put(redisIp, new Supplier<String>() {
                    @Override
                    public String get() {
                        return shard1Builder.toString();
                    }
                });
        }
        StringBuilder shard2Builder = new StringBuilder(SUBSCRIBE_HEADER);
        for (int i = 0; i < 5; i++) {
            shard2Builder.append(String.format(SENTINEL_HELLO_TEMPLATE, 5005 + i, servers.get(ACTIVE_DC_SHARD2_MASTER).getPort()));
        }
        for (String redisIp : serverResults.keySet()) {
            if (redisIp.contains("Shard2"))
                serverResults.put(redisIp, new Supplier<String>() {
                    @Override
                    public String get() {
                        return shard2Builder.toString();
                    }
                });
        }

        AtomicInteger counter = new AtomicInteger(0);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = 800;

        action.addListener(new SentinelHelloCollector() {

            @Override
            public void onAction(SentinelActionContext context) {
                try {
                    Assert.assertEquals(5, context.getResult().size());
                } catch (Throwable e) {
                    Assert.assertNotNull(context.getCause());
                }
                counter.incrementAndGet();
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }

            @Override
            public boolean worksfor(ActionContext t) {
                return true;
            }
        });
        Assert.assertEquals(1, action.getListeners().size());
        action.doTask();

        waitConditionUntilTimeOut(() -> counter.get() == 4, SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL+1000);

        Assert.assertEquals(1, servers.get(BACKUP_DC_SHARD1_SLAVE1).getConnected());
        Assert.assertEquals(1, servers.get(BACKUP_DC_SHARD1_SLAVE2).getConnected());
        Assert.assertEquals(1, servers.get(BACKUP_DC_SHARD2_SLAVE1).getConnected());
        Assert.assertEquals(1, servers.get(BACKUP_DC_SHARD2_SLAVE2).getConnected());

        action.doStop();
        sleep(100);
        Assert.assertEquals(0, servers.get(BACKUP_DC_SHARD1_SLAVE1).getConnected());
        Assert.assertEquals(0, servers.get(BACKUP_DC_SHARD1_SLAVE2).getConnected());
        Assert.assertEquals(0, servers.get(BACKUP_DC_SHARD2_SLAVE1).getConnected());
        Assert.assertEquals(0, servers.get(BACKUP_DC_SHARD2_SLAVE2).getConnected());
    }

    @Test
    public void testDoScheduleTaskWithSentinelHelloFailed() throws Exception {
        StringBuilder shard1Builder = new StringBuilder(SUBSCRIBE_HEADER);
        for (int i = 0; i < 5; i++) {
            shard1Builder.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i, servers.get(ACTIVE_DC_SHARD1_MASTER).getPort()));
        }
        serverResults.put(BACKUP_DC_SHARD1_SLAVE1, new Supplier<String>() {
            @Override
            public String get() {
                return shard1Builder.toString();
            }
        });

        StringBuilder consistentBuilder = new StringBuilder(SUBSCRIBE_HEADER);
        for (int i = 0; i < 3; i++) {
            consistentBuilder.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i, servers.get(ACTIVE_DC_SHARD1_SLAVE).getPort()));
        }
        serverResults.put(BACKUP_DC_SHARD1_SLAVE2, new Supplier<String>() {
            @Override
            public String get() {
                return consistentBuilder.toString();
            }
        });

        StringBuilder shard2Builder = new StringBuilder(SUBSCRIBE_HEADER);
        for (int i = 0; i < 3; i++) {
            shard2Builder.append(String.format(SENTINEL_HELLO_TEMPLATE, 5005 + i, servers.get(ACTIVE_DC_SHARD2_MASTER).getPort()));
        }
        serverResults.put(BACKUP_DC_SHARD2_SLAVE2, new Supplier<String>() {
            @Override
            public String get() {
                return shard2Builder.toString();
            }
        });

        servers.get(BACKUP_DC_SHARD2_SLAVE1).stop();

        AtomicInteger successResults = new AtomicInteger(0);
        AtomicInteger failedResults = new AtomicInteger(0);
        SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL = 800;

        action.addListener(new SentinelHelloCollector() {

            @Override
            public void onAction(SentinelActionContext context) {
                if (context.instance().getEndpoint().getPort() == servers.get(BACKUP_DC_SHARD1_SLAVE1).getPort()) {
                    successResults.incrementAndGet();
                    Assert.assertEquals(5, context.getResult().size());
                    Assert.assertEquals(redisMetas.get(ACTIVE_DC_SHARD1_MASTER).getPort().intValue(), context.getResult().iterator().next().getMasterAddr().getPort());
                } else if (context.instance().getEndpoint().getPort() == servers.get(BACKUP_DC_SHARD1_SLAVE2).getPort()) {
                    successResults.incrementAndGet();
                    Assert.assertEquals(3, context.getResult().size());
                    Assert.assertEquals(redisMetas.get(ACTIVE_DC_SHARD1_SLAVE).getPort().intValue(), context.getResult().iterator().next().getMasterAddr().getPort());
                } else if (context.instance().getEndpoint().getPort() == servers.get(BACKUP_DC_SHARD2_SLAVE2).getPort()) {
                    successResults.incrementAndGet();
                    Assert.assertEquals(5, context.getResult().size());
                    Assert.assertEquals(redisMetas.get(ACTIVE_DC_SHARD2_MASTER).getPort().intValue(), context.getResult().iterator().next().getMasterAddr().getPort());
                } else {
                    failedResults.incrementAndGet();
                    Assert.assertEquals(servers.get(BACKUP_DC_SHARD2_SLAVE1).getPort(), context.instance().getEndpoint().getPort());
                    Assert.assertFalse(context.isSuccess());
                }
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }

            @Override
            public boolean worksfor(ActionContext t) {
                return true;
            }
        });
        Assert.assertEquals(1, action.getListeners().size());
        action.doTask();
        waitConditionUntilTimeOut(() -> successResults.get() == 3, SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL+1000);
        waitConditionUntilTimeOut(() -> failedResults.get() == 1, SentinelHelloCheckAction.SENTINEL_COLLECT_INFO_INTERVAL+1000);
    }


    @Ignore
    @Test
    public void testSimulateSubscribe() {
        StringBuilder sb = new StringBuilder(SUBSCRIBE_HEADER);
        for (int i = 0; i < 5; i++) {
            sb.append(String.format(SENTINEL_HELLO_TEMPLATE, 5000 + i, servers.get(ACTIVE_DC_SHARD1_MASTER).getPort()));
        }
        serverResults.put(ACTIVE_DC_SHARD1_MASTER, new Supplier<String>() {
            @Override
            public String get() {
                return sb.toString();
            }
        });

        redisHealthCheckInstances.get(ACTIVE_DC_SHARD1_MASTER).getRedisSession().subscribeIfAbsent(new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                SentinelHello hello = SentinelHello.fromString(message);
                System.out.println("hello: " + hello);
            }

            @Override
            public void fail(Throwable e) {
                logger.error("[sub-failed]", e);
            }
        }, HELLO_CHANNEL);
        sleep(100);
    }

    public void prepareMetaCache() {

        ShardMeta activeDcShard1Meta = new ShardMeta();
        activeDcShard1Meta.setId(shardName1);
        activeDcShard1Meta.addRedis(redisMetas.get(ACTIVE_DC_SHARD1_MASTER));
        activeDcShard1Meta.addRedis(redisMetas.get(ACTIVE_DC_SHARD1_SLAVE));
        ShardMeta backupDcShard1Meta = new ShardMeta();
        backupDcShard1Meta.setId(shardName1);
        backupDcShard1Meta.addRedis(redisMetas.get(BACKUP_DC_SHARD1_SLAVE1));
        backupDcShard1Meta.addRedis(redisMetas.get(BACKUP_DC_SHARD1_SLAVE2));


        ShardMeta activeDcShard2Meta = new ShardMeta();
        activeDcShard2Meta.setId(shardName2);
        activeDcShard2Meta.addRedis(redisMetas.get(ACTIVE_DC_SHARD2_MASTER));
        activeDcShard2Meta.addRedis(redisMetas.get(ACTIVE_DC_SHARD2_SLAVE));
        ShardMeta backupDcShard2Meta = new ShardMeta();
        backupDcShard2Meta.setId(shardName2);
        backupDcShard2Meta.addRedis(redisMetas.get(BACKUP_DC_SHARD2_SLAVE1));
        backupDcShard2Meta.addRedis(redisMetas.get(BACKUP_DC_SHARD2_SLAVE2));

        ShardMeta activeDcShard3Meta = new ShardMeta();
        activeDcShard3Meta.setId(shardName3);
        activeDcShard3Meta.addRedis(redisMetas.get(ACTIVE_DC_SHARD3_MASTER));
        activeDcShard3Meta.addRedis(redisMetas.get(ACTIVE_DC_SHARD3_SLAVE));

        ClusterMeta activeDcClusterMeta = new ClusterMeta();
        activeDcClusterMeta.setId(clusterName);
        activeDcClusterMeta.setActiveDc("dc1");
        activeDcClusterMeta.addShard(activeDcShard1Meta);
        activeDcClusterMeta.addShard(activeDcShard2Meta);
        activeDcClusterMeta.addShard(activeDcShard3Meta);
        ClusterMeta backupDcClusterMeta = new ClusterMeta();
        backupDcClusterMeta.setId(clusterName);
        backupDcClusterMeta.setActiveDc("dc1");
        backupDcClusterMeta.addShard(backupDcShard1Meta);
        backupDcClusterMeta.addShard(backupDcShard2Meta);

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

    @Test
    public void testProcessSentinelHellos() throws Exception {
        HealthCheckActionListener listener = Mockito.mock(HealthCheckActionListener.class);
        action.addListener(listener);
        when(listener.worksfor(any())).thenReturn(true);

        HostPort master = new HostPort(LOCAL_HOST, redisMetas.get(ACTIVE_DC_SHARD1_MASTER).getPort());
        String monitorName = shardName1;
        SentinelHello sentinelHello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName);
        SentinelHello sentinelHello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName);
        SentinelHello sentinelHello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName);
        SentinelHello sentinelHello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName);
        SentinelHello sentinelHello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, monitorName);

        Map<RedisHealthCheckInstance, SentinelHelloCheckAction.SentinelHellos> hellos = Maps.newConcurrentMap();
        hellos.put(redisHealthCheckInstances.get(ACTIVE_DC_SHARD1_MASTER), action.new SentinelHellos().addSentinelHellos(Sets.newHashSet(sentinelHello1, sentinelHello2, sentinelHello3, sentinelHello4, sentinelHello5)));
        hellos.put(redisHealthCheckInstances.get(ACTIVE_DC_SHARD1_SLAVE), action.new SentinelHellos().addSentinelHellos(Sets.newHashSet(sentinelHello1, sentinelHello3, sentinelHello5)));


        Map<RedisHealthCheckInstance, Throwable> errors = new HashMap<>();
        errors.put(redisHealthCheckInstances.get(BACKUP_DC_SHARD1_SLAVE1), new ConnectTimeoutException("test"));
        errors.put(redisHealthCheckInstances.get(BACKUP_DC_SHARD1_SLAVE2), new CommandTimeoutException("test"));

        action.setHellos(hellos);
        action.setErrors(errors);

        action.processSentinelHellos();
        Thread.sleep(100);
        verify(listener, times(4)).onAction(any());
        Assert.assertTrue(action.getHellos().isEmpty());
        Assert.assertTrue(action.getErrors().isEmpty());
    }

    @Test
    public void stopTest() throws Exception {
        Map<RedisHealthCheckInstance, SentinelHelloCheckAction.SentinelHellos> sentinelHellos = new HashMap<>();
        sentinelHellos.put(newRandomRedisHealthCheckInstance(6379), action.new SentinelHellos());
        action.setHellos(sentinelHellos);
        Map<RedisHealthCheckInstance, Throwable> sentinelHelloErrors = new HashMap<>();
        sentinelHelloErrors.put(newRandomRedisHealthCheckInstance(6379), new Exception("test"));
        action.setErrors(sentinelHelloErrors);
        action.setCollecting(true);

        action.doStop();
        Assert.assertTrue(action.getHellos().isEmpty());
        Assert.assertTrue(action.getErrors().isEmpty());
        Assert.assertFalse(action.isCollecting());
    }
}