package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.exception.SentinelsException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ResetSentinelsTest extends AbstractCheckerTest {

    private ResetSentinels resetSentinels;

    @Mock
    private SentinelManager sentinelManager;
    @Mock
    private MetaCache metaCache;
    @Mock
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    @Mock
    private ScheduledExecutorService scheduled;

    private ExecutorService resetExecutor = Executors.newSingleThreadExecutor();

    @Mock
    private CheckerConfig checkerConfig;

    @Before
    public void init() throws Exception {
        resetSentinels = new ResetSentinels(new SentinelHelloCollectContext(), metaCache,
                keyedObjectPool, scheduled, resetExecutor, sentinelManager, checkerConfig);
        resetSentinels.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
    }

    @After
    public void shutdown() {
        resetExecutor.shutdownNow();
    }


    @Test
    public void halfSentinelsLostSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(new HashSet<>()).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(3, 2));

        //hellos is empty
        resetSentinels.checkReset();
        verify(checkerConfig, never()).getDefaultSentinelQuorumConfig();

        //hellos lost
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc"))).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        resetSentinels.checkReset();
        verify(checkerConfig, times(1)).getDefaultSentinelQuorumConfig();
        verify(sentinelManager, never()).slaves(any(), any());

        //over half sentinels lost slaves
        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        resetSentinels.checkReset();
        waitConditionUntilTimeOut(() -> {
            try {
                verify(checkerConfig, times(3)).getDefaultSentinelQuorumConfig();
                verify(sentinelManager, times(3)).slaves(any(), any());
                verify(metaCache, never()).getAllKeepers();
                return true;
            } catch (Throwable th) {
                return false;
            }
        },5000,100);

    }

    @Test
    public void rateLimit1() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 lost slaves and has too many keepers
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has too many keepers
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        Server master = startServer(6379, "$545\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:3\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");


        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        resetSentinels.checkReset();
        waitConditionUntilTimeOut(() -> {
            try {
                verify(sentinelManager, times(2)).reset(any(), anyString());
                verify(sentinelManager, times(1)).reset(sentinel5000, hello5000.getMonitorName());
                verify(sentinelManager, times(1)).reset(sentinel5001, hello5001.getMonitorName());
                return true;
            } catch (Throwable th) {
                logger.error("test failed", th);
                return false;
            }

        },5000,100);

        master.stop();
    }

    @Test
    public void rateLimit2() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 lost slaves and has too many keepers
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has too many keepers
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 has too many keepers
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 has too many keepers
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 has too many keepers
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        Server master = startServer(6379, "$545\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:3\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        resetSentinels.checkReset();
        waitConditionUntilTimeOut(() -> {
            try {
                verify(sentinelManager, times(2)).reset(any(), anyString());
                verify(sentinelManager, times(1)).reset(sentinel5000, hello5000.getMonitorName());
                return true;
            } catch (Throwable th) {
                logger.error("test failed", th);
                return false;
            }

        },5000,100);

        master.stop();
    }

    @Test
    public void noInvalidSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 is ok
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000))));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }

        verify(metaCache, times(5)).getAllKeepers();
        verify(sentinelManager, times(5)).slaves(any(), anyString());
        verify(sentinelManager, never()).reset(any(), anyString());
    }

    @Test
    public void masterLostSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 has too many keepers
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 is ok
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000))));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        //master lost slave 6381
        Server master = startServer(6379, "$480\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:2\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertTrue(th.getCause().getCause() instanceof SentinelsException);
            Assert.assertTrue(th.getCause().getCause().getMessage().contains("lost slaves"));
        }

        verify(metaCache, times(5)).getAllKeepers();
        verify(sentinelManager, times(5)).slaves(any(), anyString());
        verify(sentinelManager, never()).reset(any(), anyString());

        master.stop();
    }

    @Test
    public void masterRoleFailed() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 has too many keepers
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 is ok
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000))));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (Throwable th) {
            Assert.assertTrue(th.getCause().getCause() instanceof SentinelsException);
            Assert.assertTrue(th.getCause().getCause().getMessage().contains("info replication master"));
        }

        verify(metaCache, times(5)).getAllKeepers();
        verify(sentinelManager, times(5)).slaves(any(), anyString());
        verify(sentinelManager, never()).reset(any(), anyString());
    }

    @Test
    public void testTooManyKeepersNotInMaster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has too many keepers
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        //not in master
        Server master = startServer(6379, "$545\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:3\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");
        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }
        verify(sentinelManager, times(1)).reset(any(), any());
        master.stop();
    }

    @Test
    public void testTooManyKeepersPartInMaster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has too many keepers
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380),
                        new HostPort(LOCAL_HOST, 8000),
                        new HostPort(LOCAL_HOST, 8001),
                        new HostPort(LOCAL_HOST, 8002)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        //8000&8001 in master，8002 not in  master
        Server master = startServer(6379, "$614\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:4\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "slave3:ip=127.0.0.1,port=8001,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");
        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            th.printStackTrace();
            Assert.fail();
        }
        verify(sentinelManager, times(1)).reset(any(), any());
        master.stop();
    }

    @Test
    public void testTooManyKeepersInMaster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has too many keepers
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        //in master
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6382), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6382), new HostPort(LOCAL_HOST, 6380))));

        Server master = startServer(6382, "$614\r\n"+
                "# Replication\r\n"+
                "role:master\r\n"+
                "connected_slaves:4\r\n"+
                "slave0:ip=127.0.0.1,port=8000,state=online,offset=4939687459,lag=0\r\n"+
                "slave1:ip=127.0.0.1,port=6380,state=online,offset=4939687671,lag=0\r\n"+
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=4939687376,lag=1\r\n"+
                "slave3:ip=127.0.0.1,port=8001,state=online,offset=4939687459,lag=0\r\n"+
                "master_replid:7b394e1ec33430dd5a272411c77b136106befb86\r\n"+
                "master_replid2:0000000000000000000000000000000000000000\r\n"+
                "master_repl_offset:4939687965\r\n"+
                "second_repl_offset:-1\r\n"+
                "repl_backlog_active:1\r\n"+
                "repl_backlog_size:536870912\r\n"+
                "repl_backlog_first_byte_offset:4402817054\r\n"+
                "repl_backlog_histlen:536870912\r\n\r\n");

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }
        verify(sentinelManager, never()).reset(any(), any());
        master.stop();
    }

    @Test
    public void unknownSlavesNotInMaster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has unknown slave
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 6388)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });


        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        //not in master
        Server master = startServer(6379, "$545\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:3\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }

        verify(sentinelManager, times(1)).reset(any(), any());
        master.stop();

    }

    @Test
    public void unknownSlavesPartInMaster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has unknown slave
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380),
                        new HostPort(LOCAL_HOST, 8000),
                        new HostPort(LOCAL_HOST, 6388),
                        new HostPort(LOCAL_HOST, 6389)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });


        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        //not in master
        Server master = startServer(6379, "$614\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:4\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "slave3:ip=127.0.0.1,port=6388,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }

        verify(sentinelManager, times(1)).reset(any(), any());
        master.stop();

    }


    @Test
    public void unknownSlavesInMaster() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());

        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 is ok
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has unknown slave
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 6382)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        //in master
        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        Server master = startServer(6379, "$614\r\n"+
                "# Replication\r\n"+
                "role:master\r\n"+
                "connected_slaves:4\r\n"+
                "slave0:ip=127.0.0.1,port=8000,state=online,offset=4939687459,lag=0\r\n"+
                "slave1:ip=127.0.0.1,port=6380,state=online,offset=4939687671,lag=0\r\n"+
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=4939687376,lag=1\r\n"+
                "slave3:ip=127.0.0.1,port=6382,state=online,offset=4939687459,lag=0\r\n"+
                "master_replid:7b394e1ec33430dd5a272411c77b136106befb86\r\n"+
                "master_replid2:0000000000000000000000000000000000000000\r\n"+
                "master_repl_offset:4939687965\r\n"+
                "second_repl_offset:-1\r\n"+
                "repl_backlog_active:1\r\n"+
                "repl_backlog_size:536870912\r\n"+
                "repl_backlog_first_byte_offset:4402817054\r\n"+
                "repl_backlog_histlen:536870912\r\n\r\n");



        try {
            resetSentinels.checkResetCommands().execute().get(50000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }

        verify(sentinelManager, never()).reset(any(), any());
        master.stop();
    }

    @Test
    public void sentinelSlavesFailed() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(5, 3));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        SentinelHello hello5003 = new SentinelHello(new HostPort(LOCAL_HOST, 5003), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5003 = new Sentinel(hello5003.getSentinelAddr().toString(), hello5003.getSentinelAddr().getHost(), hello5003.getSentinelAddr().getPort());

        SentinelHello hello5004 = new SentinelHello(new HostPort(LOCAL_HOST, 5004), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5004 = new Sentinel(hello5004.getSentinelAddr().toString(), hello5004.getSentinelAddr().getHost(), hello5004.getSentinelAddr().getPort());

        //sentinel5000 failed
        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setFailure(new TimeoutException("timed out"));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5001 has unknown slave
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 6388)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5002 is ok
        when(sentinelManager.slaves(sentinel5002, hello5002.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5003 is ok
        when(sentinelManager.slaves(sentinel5003, hello5003.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });
        //sentinel5004 is ok
        when(sentinelManager.slaves(sentinel5004, hello5004.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 8000)));
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });


        when(sentinelManager.reset(any(), any())).thenReturn(new AbstractCommand<Long>() {
            @Override
            protected void doExecute() throws Throwable {
                future().setSuccess(1L);
            }

            @Override
            protected void doReset() {

            }

            @Override
            public String getName() {
                return null;
            }
        });

        resetSentinels.setContext(new SentinelHelloCollectContext().setToCheckReset(Sets.newHashSet(hello5000, hello5001, hello5002, hello5003, hello5004)).setSentinelMonitorName("cluster+shard+activeDc").setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        //not in master
        Server master = startServer(6379, "$545\r\n" +
                "# Replication\r\n" +
                "role:master\r\n" +
                "connected_slaves:3\r\n" +
                "slave0:ip=127.0.0.1,port=6380,state=online,offset=148954935,lag=1\r\n" +
                "slave1:ip=127.0.0.1,port=8000,state=online,offset=148955111,lag=1\r\n" +
                "slave2:ip=127.0.0.1,port=6381,state=online,offset=148955111,lag=1\r\n" +
                "master_replid:2e7638097f69cd5c3a7670dccceac87707512845\r\n" +
                "master_replid2:2d825e622e73205c8130aabc2965d3656103b3ce\r\n" +
                "master_repl_offset:148955111\r\n" +
                "second_repl_offset:120548767\r\n" +
                "repl_backlog_active:1\r\n" +
                "repl_backlog_size:104857600\r\n" +
                "repl_backlog_first_byte_offset:120501034\r\n" +
                "repl_backlog_histlen:28454078\r\n\r\n");

        try {
            resetSentinels.checkResetCommands().execute().get(5000, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            Assert.fail();
        }

        verify(sentinelManager, times(1)).reset(any(), any());
        master.stop();
    }

}
