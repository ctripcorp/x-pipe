package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;

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
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(3, 2));

        //hellos is empty
        resetSentinels.checkReset("cluster", "shard", "cluster+shard+activeDc", new HashSet<>());
        verify(checkerConfig, never()).getDefaultSentinelQuorumConfig();

        //hellos lost
        resetSentinels.checkReset("cluster", "shard", "cluster+shard+activeDc", Sets.newHashSet(new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc")));
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
        when(sentinelManager.slaves(sentinel5001, hello5001.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
            @Override
            protected void doExecute() throws Throwable {
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
            protected void doExecute() throws Throwable {
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
        resetSentinels.checkReset("cluster", "shard", "cluster+shard+activeDc", Sets.newHashSet(hello5000, hello5001, hello5002));
        verify(checkerConfig, times(3)).getDefaultSentinelQuorumConfig();
        verify(sentinelManager, times(3)).slaves(any(), any());
        verify(metaCache, never()).getAllKeepers();
    }

    @Test
    public void rateLimit() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));
        when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(new QuorumConfig(3, 2));
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        SentinelHello hello5000 = new SentinelHello(new HostPort(LOCAL_HOST, 5000), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5000 = new Sentinel(hello5000.getSentinelAddr().toString(), hello5000.getSentinelAddr().getHost(), hello5000.getSentinelAddr().getPort());

        SentinelHello hello5001 = new SentinelHello(new HostPort(LOCAL_HOST, 5001), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5001 = new Sentinel(hello5001.getSentinelAddr().toString(), hello5001.getSentinelAddr().getHost(), hello5001.getSentinelAddr().getPort());

        SentinelHello hello5002 = new SentinelHello(new HostPort(LOCAL_HOST, 5002), new HostPort(LOCAL_HOST, 6379), "cluster+shard+activeDc");
        Sentinel sentinel5002 = new Sentinel(hello5002.getSentinelAddr().toString(), hello5002.getSentinelAddr().getHost(), hello5002.getSentinelAddr().getPort());

        when(sentinelManager.slaves(sentinel5000, hello5000.getMonitorName())).thenReturn(new AbstractCommand<List<HostPort>>() {
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

        Server master = startServer(6379, "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*3\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6380\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8000\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6381\r\n"
                + "$1\r\n0\r\n");

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        resetSentinels.checkReset("cluster", "shard", "cluster+shard+activeDc", Sets.newHashSet(hello5000, hello5001, hello5002));
        waitConditionUntilTimeOut(new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                try {
                    verify(checkerConfig, times(5)).getDefaultSentinelQuorumConfig();
                    verify(sentinelManager, times(3)).slaves(any(), any());
                    verify(metaCache, times(3)).getAllKeepers();
                    verify(sentinelManager, times(1)).reset(any(), anyString());
                    return true;
                }catch (Throwable th){
                    return false;
                }

            }
        });

        master.stop();
    }

    @Test
    public void noInvalidSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380))));

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 6380)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
    }

    @Test
    public void sentinelLostSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 6380)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
    }

    @Test
    public void masterLostSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        startServer(6379,"*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*2\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6380\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8000\r\n"
                + "$1\r\n0\r\n");


        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
    }

    @Test
    public void testTooManyKeepers() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        //not in master
        Server master = startServer(6379, "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*3\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6380\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8000\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6381\r\n"
                + "$1\r\n0\r\n");

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);
        master.stop();

        //in master
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6382), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6382), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        Server master2 = startServer(6382, "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*4\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6380\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8000\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6381\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8001\r\n"
                + "$1\r\n0\r\n");

        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
        master2.stop();
    }

    @Test
    public void unknownSlaves() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6379), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        //not in master
        Server master = startServer(6379, "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*3\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6380\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8000\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6381\r\n"
                + "$1\r\n0\r\n");

        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381), new HostPort(LOCAL_HOST, 6382), new HostPort(LOCAL_HOST, 8000)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);
        master.stop();

        //in master
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort(LOCAL_HOST, 6382), new ArrayList<>()))
                .setShardInstances(Lists.newArrayList(new HostPort(LOCAL_HOST, 6379), new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381))));

        Server master2 = startServer(6382, "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":43\r\n"
                + "*4\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6380\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n8000\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6381\r\n"
                + "$1\r\n0\r\n"
                + "*3\r\n"
                + "$9\r\n127.0.0.1\r\n"
                + "$4\r\n6382\r\n"
                + "$1\r\n0\r\n");

        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 6380), new HostPort(LOCAL_HOST, 6381),new HostPort(LOCAL_HOST, 6382), new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
        master2.stop();
    }

}
