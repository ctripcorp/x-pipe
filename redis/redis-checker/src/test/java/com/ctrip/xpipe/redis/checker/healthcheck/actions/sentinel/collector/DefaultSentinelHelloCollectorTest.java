package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
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
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
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
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.net.SocketException;
import java.util.*;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector.NO_SUCH_MASTER;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@RunWith(MockitoJUnitRunner.Silent.class)
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

    private int originTimeout;

    @Before
    public void beforeDefaultSentinelCollectorTest() throws Exception {
        originTimeout = AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
        AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 500;
        HealthCheckEndpointFactory endpointFactory = mock(HealthCheckEndpointFactory.class);
        when(endpointFactory.getOrCreateEndpoint(any(HostPort.class))).thenAnswer(new Answer<Endpoint>() {
            @Override
            public Endpoint answer(InvocationOnMock invocation) throws Throwable {
                HostPort hostPort = invocation.getArgument(0, HostPort.class);
                return new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
            }
        });
        when(checkerDbConfig.shouldSentinelCheck(Mockito.any())).thenReturn(true);
        when(checkerConfig.sentinelMasterConfig()).thenReturn(new HashMap<>());
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
        AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = originTimeout;
        if (server != null) {
            server.stop();
        }
    }


    @Test
    public void checkTrueMastersTest() throws Exception {
//        meta master and hello masters all empty
        Set<SentinelHello> hellos = new HashSet<>();
        doReturn(null).when(sentinelCollector).getMaster(any());
        DefaultSentinelHelloCollector.SentinelHelloCollectorCommand command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()), hellos));
        try {
            command.new CheckTrueMaster().execute().get();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof MasterNotFoundException);
        }

//        meta master and hello masters consistent
        HostPort helloMaster = new HostPort(LOCAL_HOST, randomPort());
        doReturn(helloMaster).when(sentinelCollector).getMaster(any());
        SentinelHello hello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), helloMaster, monitorName);
        SentinelHello hello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), helloMaster, monitorName);
        SentinelHello hello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), helloMaster, monitorName);
        SentinelHello hello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), helloMaster, monitorName);
        SentinelHello hello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), helloMaster, monitorName);
        hellos = Sets.newHashSet(hello1, hello2, hello3, hello4, hello5);
        command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()),hellos));
        command.new CheckTrueMaster().execute().get();
        Assert.assertEquals(helloMaster, command.getTrueMaster());

//        meta master null and hello master consistent
        doReturn(null).when(sentinelCollector).getMaster(any());
        command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()),hellos));
        command.new CheckTrueMaster().execute().get();
        Assert.assertEquals(helloMaster, command.getTrueMaster());

//        meta master inconsistent with hello master

        //double masters
        HostPort metaMaster = new HostPort(LOCAL_HOST, randomPort());
        doReturn(metaMaster).when(sentinelCollector).getMaster(any());
        Server metaMasterServer = startServer(metaMaster.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":0\r\n*0\r\n");
        Server helloMasterServer = startServer(helloMaster.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":0\r\n*0\r\n");
        command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()), hellos));
        try {
            command.new CheckTrueMaster().execute().get();
            Assert.assertEquals(metaMaster, command.getTrueMaster());
        } catch (Exception e) {
            Assert.fail();
        }

        metaMasterServer.stop();
        //single master
        metaMaster = new HostPort(LOCAL_HOST, randomPort());
        doReturn(metaMaster).when(sentinelCollector).getMaster(any());
        command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()), hellos));
        try {
            command.new CheckTrueMaster().execute().get();
            Assert.assertEquals(helloMaster, command.getTrueMaster());
        } catch (Exception e) {
            Assert.fail();
        }

        helloMasterServer.stop();
        //no masters
        helloMaster = new HostPort(LOCAL_HOST, randomPort());
        hello1 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), helloMaster, monitorName);
        hello2 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), helloMaster, monitorName);
        hello3 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), helloMaster, monitorName);
        hello4 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), helloMaster, monitorName);
        hello5 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), helloMaster, monitorName);
        hellos = Sets.newHashSet(hello1, hello2, hello3, hello4, hello5);
        command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()), hellos));
        try {
            command.new CheckTrueMaster().execute().get();
            Assert.assertEquals(metaMaster, command.getTrueMaster());
        } catch (Exception e) {
            Assert.fail();
        }
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
        int originTimeout = AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
        try {
            AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 10;
            boolean result = sentinelCollector.isKeeperOrDead(localHostport(0));
            logger.info("{}", result);
            Assert.assertTrue(result);
        } finally {
            AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = originTimeout;
        }
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
    public void testRateLimitWorks() throws Exception {
        sentinelCollector.setScheduled(scheduled);
        when(checkerConfig.getSentinelRateLimitSize()).thenReturn(0);
        when(checkerConfig.isSentinelRateLimitOpen()).thenReturn(true);
        sentinelCollector.postConstruct();

        DefaultSentinelHelloCollector.SentinelHelloCollectorCommand command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()), Sets.newHashSet(new SentinelHello())));
        try {
            command.new AcquireLeakyBucket().execute().get();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("tryAcquire"));
        }
    }

    @Test
    public void testRateNotLimit() throws Exception {
        sentinelCollector.setScheduled(scheduled);
        when(checkerConfig.getSentinelRateLimitSize()).thenReturn(0);
        when(checkerConfig.isSentinelRateLimitOpen()).thenReturn(false);
        sentinelCollector.postConstruct();

        DefaultSentinelHelloCollector.SentinelHelloCollectorCommand command = sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(newRandomRedisHealthCheckInstance(randomPort()), Sets.newHashSet(new SentinelHello())));
        try {
            command.new AcquireLeakyBucket().execute().get();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSkipCollectForSentinelWhiteList() throws Exception {
        when(checkerDbConfig.shouldSentinelCheck(Mockito.any())).thenReturn(false);
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(instance, Collections.emptySet())).execute().addListener(commandFuture -> {
            Assert.assertTrue(commandFuture.isSuccess());
        });
        verify(sentinelCollector, never()).getMaster(any());
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

        Sentinel Sentinel5000 = new Sentinel(new HostPort(LOCAL_HOST, 5000).toString(), LOCAL_HOST, 5000);
        Sentinel Sentinel5001 = new Sentinel(new HostPort(LOCAL_HOST, 5001).toString(), LOCAL_HOST, 5001);
        Sentinel Sentinel5002 = new Sentinel(new HostPort(LOCAL_HOST, 5002).toString(), LOCAL_HOST, 5002);
        Sentinel Sentinel5003 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel Sentinel5004 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);

//        missing 5000 and 5001
        when(sentinelManager.getMasterOfMonitor(Sentinel5000, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new RedisError(NO_SUCH_MASTER));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5001, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new RedisError(NO_SUCH_MASTER));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        when(sentinelManager.getMasterOfMonitor(Sentinel5002, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(master);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5003, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(master);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5004, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(master);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        SentinelActionContext context = new SentinelActionContext(instance, new HashSet<>());
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(5)).getMasterOfMonitor(any(),any());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(2)).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());

//        5002~5004 read timed out, stop check
        when(sentinelManager.getMasterOfMonitor(Sentinel5002, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new SocketException("read timed out"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5003, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new SocketException("read timed out"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5004, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new SocketException("read timed out"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();
        verify(sentinelManager, times(10)).getMasterOfMonitor(any(),any());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(2)).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());

//        5000~5002 missing , 5003~5004 read timed out
        when(sentinelManager.getMasterOfMonitor(Sentinel5002, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new RedisError(NO_SUCH_MASTER));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5003, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new SocketException("read timed out"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5004, monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new SocketException("read timed out"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();
        verify(sentinelManager, times(15)).getMasterOfMonitor(any(),any());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
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
        Sentinel Sentinel5003 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel Sentinel5004 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);
        when(sentinelManager.getMasterOfMonitor(Sentinel5003,monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new RedisError(NO_SUCH_MASTER));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5004,monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(master);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(2)).getMasterOfMonitor(any(),any());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
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
//        doReturn(Sets.newHashSet(master)).when(sentinelCollector).checkTrueMasters(any(), any());

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
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).removeSentinelMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5005).toString(), LOCAL_HOST, 5005), monitorName);
        verify(sentinelManager, never()).monitorMaster(any(Sentinel.class), anyString(), any(HostPort.class), anyInt());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
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

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, "monitorName2")
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, never()).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).removeSentinelMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), "monitorName2");
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
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

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Server metaMasterServer = startServer(master.getPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":0\r\n*0\r\n");
        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();
        metaMasterServer.stop();
        verify(sentinelManager, never()).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
    }

    @Test
    public void sentinelSetTest() throws Exception{

        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        Map<ClusterType, String[]> clusterTypeSentinelConfig = new HashMap<>();
        clusterTypeSentinelConfig.put(ClusterType.ONE_WAY, new String[]{"failover-timeout", "60000"});
        sentinelCollector.setClusterTypeSentinelConfig(clusterTypeSentinelConfig);

        Sentinel sentinel = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);
        when(sentinelManager.getMasterOfMonitor(sentinel,monitorName)).thenReturn(new AbstractCommand<HostPort>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(new RedisError(NO_SUCH_MASTER));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(1)).getMasterOfMonitor(sentinel, monitorName);
        verify(sentinelManager, never()).removeSentinelMonitor(sentinel, monitorName);
        verify(sentinelManager, times(1)).monitorMaster(sentinel, monitorName, master,quorumConfig.getQuorum());
        verify(sentinelManager, times(1)).sentinelSet(sentinel, monitorName,clusterTypeSentinelConfig.get(ClusterType.ONE_WAY));

        clusterTypeSentinelConfig.clear();
        clusterTypeSentinelConfig.put(ClusterType.CROSS_DC, new String[]{"failover-timeout", "60000"});

        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();
        verify(sentinelManager, times(2)).getMasterOfMonitor(sentinel, monitorName);
        verify(sentinelManager, never()).removeSentinelMonitor(sentinel, monitorName);
        verify(sentinelManager, times(2)).monitorMaster(sentinel, monitorName, master,quorumConfig.getQuorum());
        verify(sentinelManager, times(1)).sentinelSet(any(), any(),any());
    }

}