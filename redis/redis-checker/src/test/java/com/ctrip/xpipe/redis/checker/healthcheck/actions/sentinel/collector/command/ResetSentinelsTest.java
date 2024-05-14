package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
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


    @Before
    public void init() throws Exception {
        resetSentinels = new ResetSentinels(new SentinelHelloCollectContext(), metaCache,
                keyedObjectPool, scheduled, resetExecutor,sentinelManager);
        resetSentinels.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
    }

    @Test
    public void testTooManyKeepers() throws Exception{
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort("localhost", 6379), new ArrayList<>())));

//        sentinelManager.slaves
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001), new HostPort(LOCAL_HOST, 8002)));

        //        1、command failed
        boolean shouldReset= resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        //        2、some keepers unreachable
        Server activeKeeper0 = startServer(8000,"*5\r\n"
                + "$6\r\nkeeper\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":6379\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        //        3、invalid keeper not connected to master
        Server activeKeeper1 = startServer(8001,"*5\r\n"
                + "$6\r\nkeeper\r\n"
                + "$10\r\nlocalhost2\r\n"
                + ":6379\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        //        4、invalid keeper connected to master
        activeKeeper1.stop();
        Server activeKeeper2 = startServer(8002,"*5\r\n"
                + "$6\r\nkeeper\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":6379\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8002)), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
        activeKeeper2.stop();
        activeKeeper0.stop();
    }

    @Test
    public void testOneWayReset() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort("localhost", 6380), new ArrayList<>())));
//        sentinelManager.slaves
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));

        HostPort wrongSlave = new HostPort("otherClusterShardSlave", 6379);
        when(metaCache.findClusterShard(wrongSlave)).thenReturn(new Pair<>("otherCluster", "otherShard"));
        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), wrongSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        Server unknownSlaveServer = startServer(8002,"*5\r\n"
                + "$5\r\nslave\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":6380\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        HostPort unknownConnectedSlave = new HostPort(LOCAL_HOST, unknownSlaveServer.getPort());
        when(metaCache.findClusterShard(unknownConnectedSlave)).thenReturn(null);
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), unknownConnectedSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);
        unknownSlaveServer.stop();

        HostPort unknownUnreachableSlave = new HostPort(LOCAL_HOST, 8003);
        when(metaCache.findClusterShard(unknownUnreachableSlave)).thenReturn(null);
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), unknownUnreachableSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        Server unknownAndConnectedToOtherMasterSlaveServer = startServer(8004,"*5\r\n"
                + "$5\r\nslave\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":6381\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        HostPort unknownAndConnectedToOtherMasterSlave = new HostPort(LOCAL_HOST, unknownAndConnectedToOtherMasterSlaveServer.getPort());
        when(metaCache.findClusterShard(unknownAndConnectedToOtherMasterSlave)).thenReturn(null);
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), unknownAndConnectedToOtherMasterSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        HostPort trueSlave = new HostPort(LOCAL_HOST, 6379);
        when(metaCache.findClusterShard(trueSlave)).thenReturn(new Pair<>("cluster", "shard"));
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), trueSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);

    }


    @Test
    public void testNotOneWayReset() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort(),ClusterType.CROSS_DC);
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()).setTrueMasterInfo(new Pair<>(new HostPort("localhost", 6380), new ArrayList<>())));

        HostPort trueSlave = new HostPort(LOCAL_HOST, 6379);
        when(metaCache.findClusterShard(trueSlave)).thenReturn(new Pair<>("cluster", "shard"));
        boolean shouldReset = resetSentinels.shouldReset(Lists.newArrayList(trueSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);

        HostPort wrongSlave = new HostPort("otherClusterShardSlave", 6379);
        when(metaCache.findClusterShard(wrongSlave)).thenReturn(new Pair<>("otherCluster", "otherShard"));
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, wrongSlave), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

        Server unknownActiveSlaveServer = startServer(randomPort(),"*5\r\n"
                + "$5\r\nslave\r\n"
                + "$9\r\nlocalhost\r\n"
                + ":6380\r\n"
                + "$9\r\nconnected\r\n"
                + ":477\r\n");
        HostPort unknownActive = new HostPort(LOCAL_HOST, unknownActiveSlaveServer.getPort());
        when(metaCache.findClusterShard(unknownActive)).thenReturn(null);
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, unknownActive), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertFalse(shouldReset);

        Server unknownActiveMasterServer = startServer(randomPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":0\r\n*0\r\n");
        unknownActive = new HostPort(LOCAL_HOST, unknownActiveMasterServer.getPort());
        shouldReset = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, unknownActive), "cluster", "shard", "cluster+shard+activeDc", new HostPort(LOCAL_HOST, 22230));
        Assert.assertTrue(shouldReset);

    }

}
