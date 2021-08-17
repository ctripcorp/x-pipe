package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class CurrentDcSentinelHelloCollectorTest extends AbstractCheckerTest {

    @InjectMocks
    private CurrentDcSentinelHelloCollector collector;

    @Mock
    protected MetaCache metaCache;

    @Mock
    private SentinelManager sentinelManager;

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
    public void setupCurrentDcSentinelHelloCollectorTest() {
        collector.setScheduled(scheduled);
        collector.setCollectExecutor(executors);
        collector.setResetExecutor(executors);
        dcId = FoundationService.DEFAULT.getDataCenter();
        sentinelMonitorName = SentinelUtil.getSentinelMonitorName(clusterId, shardId, dcId);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockMeta());
        Mockito.when(quorumConfig.getTotal()).thenReturn(5);
        Mockito.when(metaCache.getDc(masterAddr)).thenReturn(dcId);
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
        Mockito.when(metaCache.getAllKeepers()).thenReturn(Collections.emptySet());
        HostPort sentinel1 = new HostPort("10.0.0.1", 5000);
        HostPort sentinel2 = new HostPort("10.0.0.1", 5001);
        HostPort sentinel3 = new HostPort("10.0.0.1", 5002);

        Map<HostPort, Pair<String, String>> clusterShardBySentinel = new HashMap<HostPort, Pair<String, String>>() {{
            put(sentinel1, Pair.of(clusterId, shardId));
            put(sentinel2, Pair.of("other-cluster", "other-shard"));
            put(sentinel3, null);
        }};


        Mockito.doAnswer(invocation -> {
            Sentinel sentinel = invocation.getArgumentAt(0, Sentinel.class);
            Pair<String, String> clusterShard = clusterShardBySentinel.get(new HostPort(sentinel.getIp(), sentinel.getPort()));
            Mockito.when(metaCache.findClusterShard(Mockito.any())).thenReturn(clusterShard);
            return Collections.singletonList(new HostPort("127.0.0.1", 7379));
        }).when(sentinelManager).slaves(Mockito.any(), Mockito.anyString());

        Set<SentinelHello> hellos = new HashSet<>();
        hellos.add(new SentinelHello(sentinel1, masterAddr, sentinelMonitorName));
        hellos.add(new SentinelHello(sentinel2, masterAddr, sentinelMonitorName));
        hellos.add(new SentinelHello(sentinel3, masterAddr, sentinelMonitorName));

        collector.checkReset(clusterId, shardId, sentinelMonitorName, hellos);
        Thread.sleep(120);
        Mockito.verify(sentinelManager, Mockito.never())
                .reset(new Sentinel(sentinel1.toString(), sentinel1.getHost(), sentinel1.getPort()), sentinelMonitorName);
        Mockito.verify(sentinelManager, Mockito.times(1))
                .reset(new Sentinel(sentinel2.toString(), sentinel2.getHost(), sentinel2.getPort()), sentinelMonitorName);
        Mockito.verify(sentinelManager, Mockito.never())
                .reset(new Sentinel(sentinel3.toString(), sentinel3.getHost(), sentinel3.getPort()), sentinelMonitorName);
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
