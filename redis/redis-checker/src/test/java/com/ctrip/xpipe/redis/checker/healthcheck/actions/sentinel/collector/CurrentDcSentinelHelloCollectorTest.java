package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class CurrentDcSentinelHelloCollectorTest extends AbstractCheckerTest {

    @InjectMocks
    private CurrentDcSentinelHelloCollector collector;

    @Mock
    protected MetaCache metaCache;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private AlertManager alertManager;

    @Mock QuorumConfig quorumConfig;

    private String dcId = "jq", clusterId = "cluster1", shardId = "shard1";

    private String sentinelMonitorName;

    private HostPort masterAddr = new HostPort("127.0.0.1", 6379);

    private Set<HostPort> sentinels = new HashSet<HostPort>(){{
        add(new HostPort("10.0.0.1", 5000));
        add(new HostPort("10.0.0.1", 5001));
        add(new HostPort("10.0.0.1", 5002));
        add(new HostPort("10.0.0.1", 5003));
        add(new HostPort("10.0.0.1", 5004));
    }};

    @Before
    public void setupCurrentDcSentinelHelloCollectorTest() throws Exception {
        collector.setScheduled(scheduled);
        collector.setCollectExecutor(executors);
        collector.setResetExecutor(executors);
        dcId = FoundationService.DEFAULT.getDataCenter();
        sentinelMonitorName = SentinelUtil.getSentinelMonitorName(clusterId, shardId, dcId);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockMeta());
        Mockito.when(quorumConfig.getTotal()).thenReturn(5);
        Mockito.when(metaCache.getDc(masterAddr)).thenReturn(dcId);
        collector.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()).setScheduled(scheduled);
    }

    @Test
    public void testDeleted() {
        SentinelHello normalHello = new SentinelHello(sentinels.iterator().next(), masterAddr, sentinelMonitorName);
        HostPort remoteDcMAster = new HostPort("127.0.0.1", 7379);
        Mockito.when(metaCache.getDc(remoteDcMAster)).thenReturn("remote-dc");

        Set<SentinelHello> hellos = new HashSet<>();
        hellos.add(normalHello);
        hellos.add(new SentinelHello(new HostPort("11.0.0.1", 5000), masterAddr, sentinelMonitorName));
        hellos.add(new SentinelHello(sentinels.iterator().next(), remoteDcMAster, sentinelMonitorName));
        hellos.add(new SentinelHello(sentinels.iterator().next(), masterAddr, "error-monitor-name"));

        Set<SentinelHello> needDeletedHello = collector.checkStaleHellos(sentinelMonitorName, sentinels, hellos);
        logger.info("[testDeleted] {}", needDeletedHello);
        Assert.assertEquals(3, needDeletedHello.size());
        Assert.assertFalse(needDeletedHello.contains(normalHello));

        Assert.assertEquals(1, hellos.size());
        Assert.assertTrue(hellos.contains(normalHello));
    }

    @Test
    public void testReset() throws Exception {
        HostPort trueSlave=new HostPort(LOCAL_HOST,6379);
        when(metaCache.findClusterShard(trueSlave)).thenReturn(new Pair<>("cluster","shard"));
        Pair<Boolean, String> shouldResetAndReason = collector.shouldReset(Lists.newArrayList(trueSlave), "cluster", "shard");
        Assert.assertFalse(shouldResetAndReason.getKey());

        HostPort wrongSlave=new HostPort("otherClusterShardSlave",6379);
        when(metaCache.findClusterShard(wrongSlave)).thenReturn(new Pair<>("otherCluster","otherShard"));
        shouldResetAndReason = collector.shouldReset(Lists.newArrayList(trueSlave,wrongSlave), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("but meta:otherCluster:otherShard"));

        Server unknownActiveServer = startServer(randomPort(), "*3\r\n"
                + "$6\r\nslave\r\n"
                + ":0\r\n*0\r\n");
        HostPort unknownActive=new HostPort(LOCAL_HOST,unknownActiveServer.getPort());
        when(metaCache.findClusterShard(unknownActive)).thenReturn(null);
        shouldResetAndReason = collector.shouldReset(Lists.newArrayList(trueSlave,unknownActive), "cluster", "shard");
        Assert.assertFalse(shouldResetAndReason.getKey());
        verify(alertManager,times(1)).alert(anyString(), anyString(), any(), any(), anyString());

        unknownActiveServer.stop();
        shouldResetAndReason = collector.shouldReset(Lists.newArrayList(trueSlave, unknownActive), "cluster", "shard");
        Assert.assertTrue(shouldResetAndReason.getKey());
        Assert.assertTrue(shouldResetAndReason.getValue().contains("keeper or dead"));
        verify(metaCache,never()).getAllKeepers();

    }

    @Test
    public void testGetSentinelMonitorName() {
        RedisInstanceInfo info = mockInstanceInfo("127.0.0.1", 6379);
        Assert.assertEquals(String.format("%s+%s+%s", clusterId, shardId, dcId),
                collector.getSentinelMonitorName(info));
    }

    @Test
    public void testGetSentinels() {
        RedisInstanceInfo info = mockInstanceInfo("127.0.0.1", 6379);
        Set<HostPort> result = collector.getSentinels(info);
        Assert.assertEquals(sentinels, result);
    }

    @Test
    public void testGetMaster() throws Exception {
        RedisInstanceInfo info = mockInstanceInfo("127.0.0.1", 6379);
        HostPort result = collector.getMaster(info);
        Assert.assertEquals(masterAddr, result);
    }

    @Test
    public void testIsHelloMasterInWrongDc() {
        HostPort remoteDcMAster = new HostPort("127.0.0.1", 7379);
        SentinelHello hello = new SentinelHello(sentinels.iterator().next(), remoteDcMAster, sentinelMonitorName);
        Mockito.when(metaCache.getDc(remoteDcMAster)).thenReturn("remote-dc");
        Assert.assertTrue(collector.isHelloMasterInWrongDc(hello));
    }

    private RedisInstanceInfo mockInstanceInfo(String ip, int port) {
        return new DefaultRedisInstanceInfo(dcId, clusterId, shardId, new HostPort(ip, port), null,
                ClusterType.BI_DIRECTION);
    }

    private XpipeMeta mockMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta(dcId);
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId).setSentinelId(1L);
        dcMeta.addSentinel(new SentinelMeta(1L).setAddress("10.0.0.1:5000,10.0.0.1:5001,10.0.0.1:5002,10.0.0.1:5003,10.0.0.1:5004"));
        shardMeta.addRedis(new RedisMeta().setIp(masterAddr.getHost()).setPort(masterAddr.getPort()));
        return xpipeMeta.addDc(dcMeta.addCluster(clusterMeta.addShard(shardMeta)));
    }

}
