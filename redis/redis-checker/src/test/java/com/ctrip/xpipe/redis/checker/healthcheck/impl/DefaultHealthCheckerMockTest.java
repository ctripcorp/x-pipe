package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.MetaChangeManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultHealthCheckerMockTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultHealthChecker checker;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckInstanceManager instanceManager;

    @Mock
    private MetaChangeManager metaChangeManager;

    @Mock
    private CheckerConfig checkerConfig;

    @Before
    public void setupDefaultHealthCheckerMockTest() {
        Mockito.when(checkerConfig.getIgnoredHealthCheckDc()).thenReturn(Collections.emptySet());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
    }

    @Test
    public void testGenerateHealthCheckInstances() throws Exception {
        Set<HostPort> loadedRedises = new HashSet<>();
        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("127.0.0.1", 6379),
                new HostPort("127.0.0.2", 6379),
                new HostPort("127.0.0.3", 6379),
                new HostPort("127.0.0.1", 6579),
                new HostPort("127.0.0.2", 6579));

        Mockito.doAnswer(invocation -> {
            RedisMeta redis = invocation.getArgument(0, RedisMeta.class);
            HostPort redisHostPort = new HostPort(redis.getIp(), redis.getPort());
            Assert.assertTrue(expectedRedises.contains(redisHostPort));
            loadedRedises.add(redisHostPort);
            return null;
        }).when(instanceManager).getOrCreate(Mockito.any(RedisMeta.class));
        checker.doInitialize();

        Assert.assertEquals(expectedRedises, loadedRedises);
    }

    @Test
    public void generateHealthCheckInstancesForCrossDcClusters(){
        Map<String, ClusterMeta> dcClusters = new HashMap<>();
        //                单机房trocks
        ClusterMeta oyCluster = new ClusterMeta().setId("cluster").addShard(
                new ShardMeta().setId("shard1").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        dcClusters.put("oy", oyCluster);
        Assert.assertEquals("oy", checker.getMaxMasterCountDc(dcClusters));

//        多机房trocks，单分片，oy master
        ClusterMeta jqCluster = new ClusterMeta().setId("cluster").addShard(
                new ShardMeta().setId("shard1").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        dcClusters.put("jq",jqCluster);
        Assert.assertEquals("oy", checker.getMaxMasterCountDc(dcClusters));

//        多机房trocks，多分片，oy master
        oyCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        Assert.assertEquals("oy", checker.getMaxMasterCountDc(dcClusters));

//        多机房trocks，多分片，一个oy master，一个jq master
        oyCluster.removeShard("shard2");
        jqCluster.removeShard("shard2");
        oyCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard2").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        Assert.assertEquals("jq", checker.getMaxMasterCountDc(dcClusters));

//        多机房trocks，多分片，一个oy master，两个jq master
        oyCluster.addShard(new ShardMeta().setId("shard3").addRedis(new RedisMeta().setMaster("127.0.0.1")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        jqCluster.addShard(new ShardMeta().setId("shard3").addRedis(new RedisMeta().setMaster("")).addRedis(new RedisMeta().setMaster("127.0.0.1")));
        Assert.assertEquals("jq", checker.getMaxMasterCountDc(dcClusters));

    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "health-instance-load-test.xml";
    }

}
