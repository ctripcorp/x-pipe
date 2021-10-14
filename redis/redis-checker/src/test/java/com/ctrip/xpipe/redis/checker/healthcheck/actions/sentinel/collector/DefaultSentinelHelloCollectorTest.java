package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelLeakyBucket;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Sets;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultSentinelHelloCollectorTest extends AbstractCheckerTest {

    @InjectMocks
    @Spy
    private DefaultSentinelHelloCollector sentinelCollector;

    private QuorumConfig quorumConfig = new QuorumConfig(5, 3);
    private String monitorName = "shard1";
    private Set<HostPort> masterSentinels;
    private HostPort master = new HostPort("127.0.0.1", randomPort());

    private Server server;

    @Mock
    private CheckerDbConfig checkerDbConfig;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private SentinelLeakyBucket leakyBucket;

    @Before
    public void beforeDefaultSentinelCollectorTest() throws Exception {
        AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 500;
        HealthCheckEndpointFactory endpointFactory = mock(HealthCheckEndpointFactory.class);
        when(endpointFactory.getOrCreateEndpoint(any(HostPort.class))).thenAnswer(new Answer<Endpoint>() {
            @Override
            public Endpoint answer(InvocationOnMock invocation) throws Throwable {
                HostPort hostPort = invocation.getArgumentAt(0, HostPort.class);
                return new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
            }
        });
        when(checkerDbConfig.shouldSentinelCheck(Mockito.any())).thenReturn(true);
        sentinelCollector.setSessionManager(new DefaultRedisSessionManager()
                .setExecutors(executors).setScheduled(scheduled).setEndpointFactory(endpointFactory)
                .setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()));
        sentinelCollector.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
        sentinelCollector.setResetExecutor(executors);
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );

    }

    @After
    public void afterDefaultSentinelHelloCollectorTest() throws Exception {
        if (server != null) {
            server.stop();
        }
    }


    @Test
    public void checkTrueMastersTest() throws Exception {
//        meta master and hello masters all empty
        Set<SentinelHello> hellos = new HashSet<>();
        Set<HostPort> trueMasters = sentinelCollector.checkTrueMasters(null, hellos);
        Assert.assertTrue(trueMasters.isEmpty());

//        meta master and hello masters consistent
        HostPort helloMaster = new HostPort(LOCAL_HOST, randomPort());
        SentinelHello hello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), helloMaster, monitorName);
        SentinelHello hello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), helloMaster, monitorName);
        SentinelHello hello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), helloMaster, monitorName);
        SentinelHello hello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), helloMaster, monitorName);
        SentinelHello hello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), helloMaster, monitorName);
        hellos = Sets.newHashSet(hello1, hello2, hello3, hello4, hello5);

        trueMasters = sentinelCollector.checkTrueMasters(helloMaster, hellos);
        Assert.assertEquals(1, trueMasters.size());
        Assert.assertTrue(trueMasters.contains(helloMaster));

//        meta master null and hello master consistent
        trueMasters = sentinelCollector.checkTrueMasters(null, hellos);
        Assert.assertEquals(1, trueMasters.size());
        Assert.assertTrue(trueMasters.contains(helloMaster));

//        meta master inconsistent with hello master

        //double masters
        HostPort metaMaster = new HostPort(LOCAL_HOST, randomPort());

        Server metaMasterServer = startServer(metaMaster.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":" + metaMaster.getPort() + "\r\n");
        Server helloMasterServer = startServer(helloMaster.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":" + helloMaster.getPort() + "\r\n");
        trueMasters = sentinelCollector.checkTrueMasters(metaMaster, hellos);
        Assert.assertEquals(2, trueMasters.size());
        metaMasterServer.stop();


        //single master
        metaMaster = new HostPort(LOCAL_HOST, randomPort());
        trueMasters = sentinelCollector.checkTrueMasters(metaMaster, hellos);
        Assert.assertEquals(1, trueMasters.size());
        Assert.assertTrue(trueMasters.contains(helloMaster));
        helloMasterServer.stop();

        //no masters
        helloMaster = new HostPort(LOCAL_HOST, randomPort());
        hello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), helloMaster, monitorName);
        hello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), helloMaster, monitorName);
        hello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), helloMaster, monitorName);
        hello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), helloMaster, monitorName);
        hello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), helloMaster, monitorName);
        hellos = Sets.newHashSet(hello1, hello2, hello3, hello4, hello5);
        trueMasters = sentinelCollector.checkTrueMasters(metaMaster, hellos);
        Assert.assertEquals(0, trueMasters.size());
    }

    @Test
    public void testAdd() {

        Set<SentinelHello> hellos = Sets.newHashSet(
                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName)
        );

        Set<SentinelHello> toAdd = sentinelCollector.checkToAdd("cluster1", "shard1", monitorName, masterSentinels, hellos, master, quorumConfig);
        Assert.assertEquals(4, toAdd.size());


        quorumConfig.setTotal(3);
        toAdd = sentinelCollector.checkToAdd("cluster1", "shard1", monitorName, masterSentinels, hellos, master, quorumConfig);
        Assert.assertEquals(2, toAdd.size());

    }

    @Test
    public void testDelete() {
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(false);

        Set<SentinelHello> hellos = Sets.newHashSet(

                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), master, monitorName)

        );

        Set<SentinelHello> toDelete = sentinelCollector.checkStaleHellos(monitorName, masterSentinels, hellos);

        Assert.assertEquals(0, toDelete.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName + "_1"));
        toDelete = sentinelCollector.checkStaleHellos(monitorName, masterSentinels, hellos);
        Assert.assertEquals(1, toDelete.size());
        Assert.assertEquals(5, hellos.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 6000), master, monitorName));
        toDelete = sentinelCollector.checkStaleHellos(monitorName, masterSentinels, hellos);
        Assert.assertEquals(1, toDelete.size());
        Assert.assertEquals(5, hellos.size());


    }

    @Test
    public void testIsKeeperOrDead() {
        AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 10;
        boolean result = sentinelCollector.isKeeperOrDead(localHostport(0));
        logger.info("{}", result);
        Assert.assertTrue(result);
    }

    @Test
    @Ignore
    public void testCorrectWhenDR() throws Exception {
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(false);
        doCallRealMethod().when(sentinelCollector).onAction(any(SentinelActionContext.class));
        doReturn(null).when(sentinelCollector).checkStaleHellos(anyString(), any(), any());
        doNothing().when(sentinelCollector).checkReset(anyString(), any(), any(), any());
        doReturn(null).when(sentinelCollector).checkToAdd(anyString(), any(), any(), any(), any(), any(), any());
//        doNothing().when(sentinelCollector).doAction(any(), any(), any());
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Set<SentinelHello> hellos = Sets.newHashSet();
        for (int i = 0; i < 5; i++) {
            hellos.add(SentinelHello.fromString(String.format("127.0.0.1,%d,d156c06308a5e5c6edba1f8786b32e22cfceafcc,8410,shard,127.0.0.1,16379,0", 500 + i)));
        }
        sentinelCollector.onAction(new SentinelActionContext(instance, hellos));
        verify(sentinelCollector, never()).checkStaleHellos(anyString(), any(), any());
    }

    // for whom reading this code, here's how and why all this happens:
    // 1. if the sentinel has already failover a master-slave, yet, we didn't do the RedisMasterCheck, we will keep an invalid data(incorrect master-slave info)
    // 2. the collector would get an empty set of Sentinel Hellos, as now the keeper were still in touch with the previous master
    // And so on so forth, backup(DR) site redises receive message from keeper, which, apparently will be nothing
    // 3. A protection collection will be triggered if an empty Sentinel Hello set is received
    @Test
    @Ignore
    public void testEmptySentinelLogDueToDoubleMasterInOneShard() throws Exception {
        String clusterId = "clusterId", shardId = "shardId";
        monitorName = shardId;
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );
        sentinelCollector = spy(sentinelCollector);
        server = startServer(master.getPort(), "*5\r\n"
                + "$6\r\nkeeper\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":6379\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        Set<SentinelHello> hellos = sentinelCollector.checkToAdd(clusterId, shardId, monitorName, masterSentinels,
                Sets.newHashSet(), master, quorumConfig);

        Assert.assertTrue(hellos.isEmpty());
    }

    @Test
    public void testEmptySentinelLogDueToDoubleMasterInOneShard2() throws Exception {
        String clusterId = "clusterId", shardId = "shardId";
        monitorName = shardId;
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );
        server = startServer(master.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6479\r\n"
                + "$1\r\n0\r\n");
        Set<SentinelHello> hellos = sentinelCollector.checkToAdd(clusterId, shardId, monitorName, masterSentinels,
                Sets.newHashSet(), master, quorumConfig);

        Assert.assertEquals(masterSentinels.size(), hellos.size());
    }

    @Test
    public void testMasterNotInPrimaryDc() {
        String shardId = "shardId";
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(true);

        monitorName = shardId;
        Set<SentinelHello> hellos = Sets.newHashSet(
                new SentinelHello(new HostPort("127.0.0.1", 5000), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), new HostPort("127.0.0.3", 6379), monitorName)
        );
        Set<SentinelHello> toDeleted = sentinelCollector.checkStaleHellos(monitorName, masterSentinels, hellos);
        Assert.assertEquals(5, toDeleted.size());
    }

    @Test
    public void testRateLimitWorks() {
        sentinelCollector.setScheduled(scheduled);
        doNothing().when(sentinelManager).removeSentinelMonitor(any(), anyString());
        when(checkerConfig.getSentinelRateLimitSize()).thenReturn(0);
        when(checkerConfig.isSentinelRateLimitOpen()).thenReturn(true);
        sentinelCollector.postConstruct();
        sentinelCollector.doAction("monitor", new HostPort("127.0.0.1", 6379), Sets.newHashSet(new SentinelHello()), Sets.newHashSet(), new QuorumConfig(5, 3));
        verify(sentinelManager, never()).removeSentinelMonitor(any(), anyString());
    }

    @Test
    public void testRateNotLimit() {
        sentinelCollector.setScheduled(scheduled);
        doNothing().when(sentinelManager).removeSentinelMonitor(any(), anyString());
        when(checkerConfig.getSentinelRateLimitSize()).thenReturn(0);
        when(checkerConfig.isSentinelRateLimitOpen()).thenReturn(false);
        sentinelCollector.postConstruct();
        sentinelCollector.doAction("monitor", new HostPort("127.0.0.1", 6379),
                Sets.newHashSet(new SentinelHello(new HostPort("127.0.0.1", 5050), new HostPort("127.0.0.1", 6379), "monitorName")), Sets.newHashSet(), new QuorumConfig(5, 3));
        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), anyString());
    }

    @Test
    public void testSkipCollectForSentinelWhiteList() throws Exception {
        when(checkerDbConfig.shouldSentinelCheck(Mockito.any())).thenReturn(false);
        when(checkerConfig.isSentinelRateLimitOpen()).thenReturn(false);

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        sentinelCollector.onAction(new SentinelActionContext(instance, Collections.emptySet()));
        verify(sentinelManager, never()).monitorMaster(any(), any(), any(), anyInt());
    }

    @Test
    public void checkWrongHelloMastersTest() throws Exception {
        Set<SentinelHello> hellos = Sets.newHashSet(
                new SentinelHello(new HostPort("127.0.0.1", 5000), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), new HostPort("127.0.0.3", 6380), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), new HostPort("127.0.0.3", 6381), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), new HostPort("127.0.0.3", 6379), monitorName)
        );

        HostPort trueMaster = new HostPort("127.0.0.3", 6379);
        Set<SentinelHello> wrongHellos = sentinelCollector.checkWrongMasterHellos(hellos, trueMaster);
        Assert.assertEquals(3, hellos.size());
        Assert.assertEquals(2, wrongHellos.size());
    }

    @Test
    public void noSentinelsTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        SentinelActionContext context = new SentinelActionContext(instance, new HashSet<>());
        sentinelCollector.collect(context);

        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(5)).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());
    }

    @Test
    public void sentinelLessThanAllTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        when(sentinelManager.getMasterOfMonitor(any(Sentinel.class), anyString())).thenReturn(new HostPort(LOCAL_HOST, randomPort()));
        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.collect(context);

        verify(sentinelManager, never()).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(2)).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
    }

    @Test
    public void wrongSentinelTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5005), master, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.collect(context);

        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).removeSentinelMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5005).toString(), LOCAL_HOST, 5005), monitorName);
        verify(sentinelManager, never()).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());
    }

    @Test
    public void wrongMonitorNameTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, "monitorName2")
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.collect(context);

        verify(sentinelManager, never()).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).removeSentinelMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), "monitorName2");
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
    }

    @Test
    public void wrongMasterTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.collect(context);

        verify(sentinelManager, never()).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).removeSentinelMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName);
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
    }

    @Test
    public void inFailoverProcessTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        when(sentinelManager.getMasterOfMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(),LOCAL_HOST,5004),monitorName)).thenReturn(master);
        HostPort oldMaster = new HostPort(LOCAL_HOST, randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), oldMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.collect(context);

        verify(sentinelManager, times(1)).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
    }

}