package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelLeakyBucket;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.AcquireLeakyBucket;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.SentinelHelloCollectContext;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.error.RedisError;
import com.ctrip.xpipe.redis.core.protocal.pojo.DefaultSentinelMasterInstance;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelFlag;
import com.ctrip.xpipe.redis.core.protocal.pojo.SentinelMasterInstance;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
    private Map<String, String> sentinelMasterInfo = new HashMap<>();
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

    @Mock
    private PersistenceCache persistenceCache;

    private int originTimeout;

    private static final String masterInfoReplication = "$481\r\n" +
            "# Replication\r\n" +
            "role:master\r\n" +
            "connected_slaves:2\r\n" +
            "slave0:ip=127.0.0.1,port=20001,state=online,offset=148954935,lag=1\r\n" +
            "slave1:ip=127.0.0.1,port=6380,state=online,offset=148955111,lag=1\r\n" +
            "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
            "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
            "master_repl_offset:148955111\r\n" +
            "second_repl_offset:120548767\r\n" +
            "repl_backlog_active:1\r\n" +
            "repl_backlog_size:104857600\r\n" +
            "repl_backlog_first_byte_offset:120501034\r\n" +
            "repl_backlog_histlen:28454078\r\n\r\n";

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
        sentinelCollector.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
        sentinelCollector.setResetExecutor(executors);
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );
        when(persistenceCache.isClusterOnMigration(anyString())).thenReturn(false);
        sentinelMasterInfo.put("name", monitorName);
        sentinelMasterInfo.put("ip", master.getHost());
        sentinelMasterInfo.put("port", String.valueOf(master.getPort()));
        sentinelMasterInfo.put("flags", "master");
    }

    @After
    public void afterDefaultSentinelHelloCollectorTest() throws Exception {
        AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = originTimeout;
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testRateLimitWorks() throws Exception {
        sentinelCollector.setScheduled(scheduled);
        when(checkerConfig.getSentinelRateLimitSize()).thenReturn(0);
        when(checkerConfig.isSentinelRateLimitOpen()).thenReturn(true);
        sentinelCollector.postConstruct();
        leakyBucket = new SentinelLeakyBucket(checkerConfig, scheduled);
        leakyBucket.start();
        try {
            new AcquireLeakyBucket(new SentinelHelloCollectContext(), leakyBucket).execute().get();
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

        leakyBucket = new SentinelLeakyBucket(checkerConfig, scheduled);
        leakyBucket.start();
        try {
            new AcquireLeakyBucket(new SentinelHelloCollectContext(), leakyBucket).execute().get();
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
        verify(persistenceCache,never()).isClusterOnMigration(any());
        verify(sentinelManager, never()).monitorMaster(any(), any(), any(), anyInt());
    }

    @Test
    public void testSkipMigratingCluster() throws Exception {
        when(checkerDbConfig.shouldSentinelCheck(Mockito.any())).thenReturn(true);
        when(persistenceCache.isClusterOnMigration(anyString())).thenReturn(true);
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        sentinelCollector.new SentinelHelloCollectorCommand(new SentinelActionContext(instance, Collections.emptySet())).execute().addListener(commandFuture -> {
            Assert.assertTrue(commandFuture.isSuccess());
        });
        verify(persistenceCache,times(1)).isClusterOnMigration(any());
        verify(sentinelManager, never()).monitorMaster(any(), any(), any(), anyInt());
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5000, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5001, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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

        when(sentinelManager.getMasterOfMonitor(Sentinel5002, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5003, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(Sentinel5004, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5002, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5003, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5004, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5002, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5003, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5004, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5003,monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
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
        when(sentinelManager.getMasterOfMonitor(Sentinel5004,monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
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
        when(metaCache.getRedisOfDcClusterShard(dc,cluster,shard)).thenReturn(Lists.newArrayList());

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5005), master, monitorName)
        );

        when(sentinelManager.removeSentinelMonitor(any(),any())).thenReturn(new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
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
        when(metaCache.getRedisOfDcClusterShard(dc, cluster, shard)).thenReturn(Lists.newArrayList());
        when(sentinelManager.removeSentinelMonitor(any(), any())).thenReturn(new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.monitorMaster(any(), any(), any(), anyInt())).thenReturn(new AbstractCommand<String>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess();
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
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, "monitorName2")
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(1)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(1)).removeSentinelMonitor(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), "monitorName2");
        verify(sentinelManager, times(1)).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
    }

    @Test
    public void sentinelPubTooManyMasters() throws Exception{
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
        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();
        verify(sentinelManager, never()).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, never()).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
    }

    @Test
    public void failoverInProgressTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);

        Sentinel sentinel3 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel sentinel1 = new Sentinel(new HostPort(LOCAL_HOST, 5001).toString(), LOCAL_HOST, 5001);
        Sentinel sentinel2 = new Sentinel(new HostPort(LOCAL_HOST, 5002).toString(), LOCAL_HOST, 5002);
        Sentinel sentinel0 = new Sentinel(new HostPort(LOCAL_HOST, 5000).toString(), LOCAL_HOST, 5000);
        Sentinel sentinel4 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);

        Map<String,String> otherSentinelMasterInfo=new HashMap<>(sentinelMasterInfo);
        when(sentinelManager.getMasterOfMonitor(sentinel2, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(otherSentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel0, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(otherSentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel1, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(otherSentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel4, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(otherSentinelMasterInfo)));

        sentinelMasterInfo.put("flags", SentinelFlag.master + "," + SentinelFlag.failover_in_progress);
        when(sentinelManager.getMasterOfMonitor(sentinel3, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();
        verify(sentinelManager, times(5)).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, never()).monitorMaster(new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004), monitorName, master, quorumConfig.getQuorum());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());
    }

    @Test
    public void masterLoseSlavesAndDeleteAllSentinelsTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        when(metaCache.getRedisOfDcClusterShard(any(), any(), any())).thenReturn(Lists.newArrayList(new RedisMeta().setIp(LOCAL_HOST).setPort(20001), new RedisMeta().setIp(LOCAL_HOST).setPort(20002),new RedisMeta().setIp(LOCAL_HOST).setPort(master.getPort())));

        Sentinel sentinel3 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel sentinel1 = new Sentinel(new HostPort(LOCAL_HOST, 5001).toString(), LOCAL_HOST, 5001);
        Sentinel sentinel2 = new Sentinel(new HostPort(LOCAL_HOST, 5002).toString(), LOCAL_HOST, 5002);
        Sentinel sentinel0 = new Sentinel(new HostPort(LOCAL_HOST, 5000).toString(), LOCAL_HOST, 5000);
        Sentinel sentinel4 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);

        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        int port = wrongMaster.getPort();
        sentinelMasterInfo.put("flags", SentinelFlag.master.name());
        sentinelMasterInfo.put("port", String.valueOf(port));


        when(sentinelManager.getMasterOfMonitor(sentinel2, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(sentinel0, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(sentinel1, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(sentinel3, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.getMasterOfMonitor(sentinel4, monitorName)).thenReturn(new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(new DefaultSentinelMasterInstance(sentinelMasterInfo));
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
        Server metaMasterServer = startServer(master.getPort(), masterInfoReplication);

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(5)).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
        verify(sentinelManager, never()).monitorMaster(any(), any(), any(), anyInt());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());

        metaMasterServer.stop();
    }

    @Test
    public void masterHasAllSlavesAndDeleteAllSentinelsTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        when(metaCache.getRedisOfDcClusterShard(any(), any(), any())).thenReturn(Lists.newArrayList(new RedisMeta().setIp(LOCAL_HOST).setPort(20001),new RedisMeta().setIp(LOCAL_HOST).setPort(master.getPort())));

        Sentinel sentinel3 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel sentinel1 = new Sentinel(new HostPort(LOCAL_HOST, 5001).toString(), LOCAL_HOST, 5001);
        Sentinel sentinel2 = new Sentinel(new HostPort(LOCAL_HOST, 5002).toString(), LOCAL_HOST, 5002);
        Sentinel sentinel0 = new Sentinel(new HostPort(LOCAL_HOST, 5000).toString(), LOCAL_HOST, 5000);
        Sentinel sentinel4 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);

        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        int port = wrongMaster.getPort();
        sentinelMasterInfo.put("flags", SentinelFlag.master.name());
        sentinelMasterInfo.put("port", String.valueOf(port));


        when(sentinelManager.getMasterOfMonitor(sentinel2, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel0, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel1, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel3, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel4, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Server metaMasterServer = startServer(master.getPort(), masterInfoReplication);

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(5)).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, times(5)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(5)).monitorMaster(any(), any(), any(), anyInt());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());

        metaMasterServer.stop();
    }

    @Test
    public void masterLoseSlavesAndDeletePartialSentinelsTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        when(metaCache.getRedisOfDcClusterShard(any(), any(), any())).thenReturn(Lists.newArrayList(new RedisMeta().setIp(LOCAL_HOST).setPort(20001), new RedisMeta().setIp(LOCAL_HOST).setPort(20002),new RedisMeta().setIp(LOCAL_HOST).setPort(master.getPort())));

        Sentinel sentinel3 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel sentinel1 = new Sentinel(new HostPort(LOCAL_HOST, 5001).toString(), LOCAL_HOST, 5001);
        Sentinel sentinel2 = new Sentinel(new HostPort(LOCAL_HOST, 5002).toString(), LOCAL_HOST, 5002);
        Sentinel sentinel0 = new Sentinel(new HostPort(LOCAL_HOST, 5000).toString(), LOCAL_HOST, 5000);
        Sentinel sentinel4 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);

        sentinelMasterInfo.put("flags", SentinelFlag.master.name());
        sentinelMasterInfo.put("port",String.valueOf(master.getPort()));
        Map<String, String> other = new HashMap<>(sentinelMasterInfo);
        when(sentinelManager.getMasterOfMonitor(sentinel2, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(other)));

        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        int port = wrongMaster.getPort();
        sentinelMasterInfo.put("port", String.valueOf(port));
        when(sentinelManager.getMasterOfMonitor(sentinel0, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel1, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel3, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel4, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Server metaMasterServer = startServer(master.getPort(), masterInfoReplication);

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(5)).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, times(4)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(4)).monitorMaster(any(), any(), any(), anyInt());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());

        metaMasterServer.stop();
    }

    @Test
    public void masterHasAllSlavesAndDeletePartialSentinelsTest() throws Exception {
        String cluster = "cluster";
        String shard = "shard";
        String dc = "dc";
        when(checkerDbConfig.shouldSentinelCheck(cluster)).thenReturn(true);
        when(metaCache.getSentinelMonitorName(cluster, shard)).thenReturn(monitorName);
        when(metaCache.getActiveDcSentinels(cluster, shard)).thenReturn(masterSentinels);
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        when(metaCache.findMaster(cluster, shard)).thenReturn(master);
        when(leakyBucket.tryAcquire()).thenReturn(true);
        when(metaCache.getRedisOfDcClusterShard(any(), any(), any())).thenReturn(Lists.newArrayList(new RedisMeta().setIp(LOCAL_HOST).setPort(20001),new RedisMeta().setIp(LOCAL_HOST).setPort(master.getPort())));

        Sentinel sentinel3 = new Sentinel(new HostPort(LOCAL_HOST, 5003).toString(), LOCAL_HOST, 5003);
        Sentinel sentinel1 = new Sentinel(new HostPort(LOCAL_HOST, 5001).toString(), LOCAL_HOST, 5001);
        Sentinel sentinel2 = new Sentinel(new HostPort(LOCAL_HOST, 5002).toString(), LOCAL_HOST, 5002);
        Sentinel sentinel0 = new Sentinel(new HostPort(LOCAL_HOST, 5000).toString(), LOCAL_HOST, 5000);
        Sentinel sentinel4 = new Sentinel(new HostPort(LOCAL_HOST, 5004).toString(), LOCAL_HOST, 5004);

        sentinelMasterInfo.put("flags", SentinelFlag.master.name());
        sentinelMasterInfo.put("port",String.valueOf(master.getPort()));
        Map<String, String> other = new HashMap<>(sentinelMasterInfo);
        when(sentinelManager.getMasterOfMonitor(sentinel2, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(other)));

        HostPort wrongMaster = new HostPort(LOCAL_HOST, randomPort());
        int port = wrongMaster.getPort();
        sentinelMasterInfo.put("port", String.valueOf(port));
        when(sentinelManager.getMasterOfMonitor(sentinel0, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel1, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel3, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));
        when(sentinelManager.getMasterOfMonitor(sentinel4, monitorName)).thenReturn(createSuccessCommand(new DefaultSentinelMasterInstance(sentinelMasterInfo)));

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Server metaMasterServer = startServer(master.getPort(), masterInfoReplication);

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), wrongMaster, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), wrongMaster, monitorName)
        );
        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        sentinelCollector.new SentinelHelloCollectorCommand(context).execute().get();

        verify(sentinelManager, times(5)).getMasterOfMonitor(any(Sentinel.class), anyString());
        verify(sentinelManager, times(4)).removeSentinelMonitor(any(), any());
        verify(sentinelManager, times(4)).monitorMaster(any(), any(), any(), anyInt());
        verify(sentinelManager, never()).sentinelSet(any(), anyString(), any());

        metaMasterServer.stop();
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
        when(sentinelManager.getMasterOfMonitor(sentinel,monitorName)).thenReturn(createFailedCommand(new RedisError(NO_SUCH_MASTER)));
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


    AbstractCommand<SentinelMasterInstance> createSuccessCommand(SentinelMasterInstance instance){
        return new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(instance);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        };
    }

    AbstractCommand<SentinelMasterInstance> createFailedCommand(Throwable e){
       return new AbstractCommand<SentinelMasterInstance>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setFailure(e);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        };
    }
}