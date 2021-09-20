package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CurrentDcSentinelHelloAggregationCollectorTest extends AbstractCheckerTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private CurrentDcSentinelHelloCollector sentinelHelloCollector;

    @Mock
    private RedisHealthCheckInstance instance;

    private CurrentDcSentinelHelloAggregationCollector collector;

    private HostPort masterAddr = new HostPort("127.0.0.1", 6379);
    private HostPort slaveAddr = new HostPort("127.0.0.1", 7379);

    private HostPort sentinel = new HostPort("10.0.0.1", 5000);

    private String dcId = "jq", clusterId = "cluster1", shardId = "shard1";

    @Before
    public void setupCurrentDcSentinelHelloAggregationCollectorTest() {
        dcId = FoundationService.DEFAULT.getDataCenter();
        collector = new CurrentDcSentinelHelloAggregationCollector(metaCache, sentinelHelloCollector, clusterId, shardId);
        Mockito.when(instance.getCheckInfo())
                .thenReturn(new DefaultRedisInstanceInfo(dcId, clusterId, shardId, slaveAddr, null, ClusterType.BI_DIRECTION));
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(mockMeta());
    }

    @Test
    public void testOnAction() {
        SentinelActionContext context = new SentinelActionContext(instance, Collections.singleton(new SentinelHello(sentinel, masterAddr, "")));
        Mockito.doAnswer(invocation -> {
            SentinelActionContext paramContext = invocation.getArgument(0, SentinelActionContext.class);
            Assert.assertEquals(Collections.singleton(new SentinelHello(sentinel, masterAddr, "")), paramContext.getResult());
            Assert.assertEquals(instance, paramContext.instance());
            return null;
        }).when(sentinelHelloCollector).onAction(Mockito.any());


        collector.onAction(context);
        Mockito.verify(sentinelHelloCollector, Mockito.times(1)).onAction(Mockito.any());
    }

    @Test
    public void testOnActionWithException() {
        SentinelActionContext context = new SentinelActionContext(instance, new Exception());
        collector.onAction(context);
        Mockito.verify(sentinelHelloCollector, Mockito.never()).onAction(Mockito.any());
    }

    private XpipeMeta mockMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta(dcId);
        ClusterMeta clusterMeta = new ClusterMeta(clusterId);
        ShardMeta shardMeta = new ShardMeta(shardId);
        shardMeta.addRedis(new RedisMeta().setIp(masterAddr.getHost()).setPort(masterAddr.getPort()))
                .addRedis(new RedisMeta().setIp(slaveAddr.getHost()).setPort(slaveAddr.getPort()).setMaster(masterAddr.toString()));

        return xpipeMeta.addDc(dcMeta.addCluster(clusterMeta.addShard(shardMeta)));
    }

}
