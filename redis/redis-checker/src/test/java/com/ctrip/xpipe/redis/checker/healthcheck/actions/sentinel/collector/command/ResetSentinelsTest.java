package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ResetSentinelsTest extends AbstractCheckerTest {

    private ResetSentinels resetSentinels;

    @Mock
    private SentinelManager sentinelManager;
    @Mock
    private MetaCache metaCache;
    @Mock
    private AlertManager alertManager;
    @Mock
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    @Mock
    private ScheduledExecutorService scheduled;
    @Mock
    private ExecutorService resetExecutor;


    @Before
    public void init() throws Exception {
        resetSentinels = new ResetSentinels(new SentinelHelloCollectContext(), metaCache, alertManager,
                keyedObjectPool, scheduled, resetExecutor,sentinelManager);
        resetSentinels.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
    }

    @Test
    public void testOneWayReset() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()));

//        sentinelManager.slaves
        when(metaCache.getAllKeepers()).thenReturn(Sets.newHashSet(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)));
        Pair<Boolean, String> shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), new HostPort(LOCAL_HOST, 8001)), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("has 2 keepers"));

        HostPort wrongSlave = new HostPort("otherClusterShardSlave", 6379);
        when(metaCache.findClusterShard(wrongSlave)).thenReturn(new Pair<>("otherCluster", "otherShard"));
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), wrongSlave), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("but meta:otherCluster:otherShard"));

        Server unknownKeeperServer = startServer(randomPort(), "*3\r\n"
                + "$6\r\nkeeper\r\n"
                + ":0\r\n*0\r\n");
        HostPort unknownKeeper = new HostPort(LOCAL_HOST, unknownKeeperServer.getPort());
        when(metaCache.findClusterShard(unknownKeeper)).thenReturn(null);
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), unknownKeeper), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("keeper or master"));

        unknownKeeperServer.stop();
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), unknownKeeper), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("keeper or master"));

        HostPort trueSlave = new HostPort(LOCAL_HOST, 6379);
        when(metaCache.findClusterShard(trueSlave)).thenReturn(new Pair<>("cluster", "shard"));
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(new HostPort(LOCAL_HOST, 8000), trueSlave), "cluster", "shard");
        Assert.assertFalse(shouldResetAndReason.getKey());

    }

    @Test
    public void testOneWayIsRedundantInstance() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort());
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()));
        int originTimeout = AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;
        try {
            AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = 10;
            boolean result = resetSentinels.redundantInstance(localHostport(0));
            logger.info("{}", result);
            Assert.assertTrue(result);
        } finally {
            AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI = originTimeout;
        }
    }

    @Test
    public void testNotOneWayReset() throws Exception {
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance("currentDc", "activeDc", randomPort(),ClusterType.CROSS_DC);
        resetSentinels.setContext(new SentinelHelloCollectContext().setInfo(instance.getCheckInfo()));

        HostPort trueSlave = new HostPort(LOCAL_HOST, 6379);
        when(metaCache.findClusterShard(trueSlave)).thenReturn(new Pair<>("cluster", "shard"));
        Pair<Boolean, String> shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(trueSlave), "cluster", "shard");
        Assert.assertFalse(shouldResetAndReason.getKey());

        HostPort wrongSlave = new HostPort("otherClusterShardSlave", 6379);
        when(metaCache.findClusterShard(wrongSlave)).thenReturn(new Pair<>("otherCluster", "otherShard"));
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, wrongSlave), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("but meta:otherCluster:otherShard"));

        Server unknownActiveSlaveServer = startServer(randomPort(), "*3\r\n"
                + "$6\r\nslave\r\n"
                + ":0\r\n*0\r\n");
        HostPort unknownActive = new HostPort(LOCAL_HOST, unknownActiveSlaveServer.getPort());
        when(metaCache.findClusterShard(unknownActive)).thenReturn(null);
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, unknownActive), "cluster", "shard");
        Assert.assertFalse(shouldResetAndReason.getKey());
        verify(alertManager, times(1)).alert(anyString(), anyString(), any(), any(), anyString());


        Server unknownActiveMasterServer = startServer(randomPort(), "*3\r\n"
                + "$6\r\nmaster\r\n"
                + ":0\r\n*0\r\n");
        unknownActive = new HostPort(LOCAL_HOST, unknownActiveMasterServer.getPort());
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, unknownActive), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        verify(alertManager, times(1)).alert(anyString(), anyString(), any(), any(), anyString());

        unknownActiveSlaveServer.stop();
        unknownActiveMasterServer.stop();
        waitConditionUntilTimeOut(() -> unknownActiveSlaveServer.getConnected() <= 0);
        shouldResetAndReason = resetSentinels.shouldReset(Lists.newArrayList(trueSlave, unknownActive), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("keeper or master"));
        verify(metaCache, never()).getAllKeepers();

    }

}
