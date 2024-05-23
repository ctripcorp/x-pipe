package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.when;

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
    @Mock
    private ExecutorService resetExecutor;
    @Mock
    private CheckerConfig checkerConfig;


    @Before
    public void init() throws Exception {
        resetSentinels = new ResetSentinels(new SentinelHelloCollectContext(), metaCache,
                keyedObjectPool, scheduled, resetExecutor,sentinelManager,checkerConfig);
        resetSentinels.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
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
