package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
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
