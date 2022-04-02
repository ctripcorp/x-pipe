package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class DefaultDcMetaChangeManagerTest extends AbstractRedisTest {

    private DefaultDcMetaChangeManager manager;

    @Mock
    private HealthCheckInstanceManager instanceManager;
    
    @Mock
    private HealthCheckEndpointFactory factory;

    @Mock
    private MetaCache metaCache;

    @Mock
    private CheckerConfig checkerConfig;

    private RedisHealthCheckInstance instance = null;

    private Set<HostPort> addedRedises = new HashSet<>();

    private Set<HostPort> deletedRedised = new HashSet<>();

    @Before
    public void beforeDefaultDcMetaChangeManagerTest() {
        MockitoAnnotations.initMocks(this);
        Mockito.doAnswer(invocation -> {
            RedisMeta redis = invocation.getArgument(0, RedisMeta.class);
            HostPort redisHostPort = new HostPort(redis.getIp(), redis.getPort());
            addedRedises.add(redisHostPort);
            return instance;
        }).when(instanceManager).getOrCreate(any(RedisMeta.class));
        Mockito.doAnswer(invocation -> {
            HostPort redis = invocation.getArgument(0, HostPort.class);
            deletedRedised.add(redis);
            return null;
        }).when(instanceManager).remove(any(HostPort.class));
        
        manager = new DefaultDcMetaChangeManager("oy", instanceManager, factory);
    }

    private void prepareData(String dc) {
        manager.compare(getDcMeta(dc));
    }

    private DcMeta cloneDcMeta(String dc) {
        DcMeta dcMeta = MetaClone.clone(getDcMeta(dc));
        for (ClusterMeta clusterMeta: dcMeta.getClusters().values()) {
            clusterMeta.setParent(dcMeta);
            for (ShardMeta shardMeta: clusterMeta.getShards().values()) {
                shardMeta.setParent(clusterMeta);
                for (RedisMeta redisMeta: shardMeta.getRedises()) {
                    redisMeta.setParent(shardMeta);
                }
                for (KeeperMeta keeperMeta: shardMeta.getKeepers()) {
                    keeperMeta.setParent(shardMeta);
                }
            }
        }
        return dcMeta;
    }

    @Test
    public void visitAddOneWayCluster() {
        manager.visitAdded(getDcMeta("oy").findCluster("cluster2"));
        verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));

        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("127.0.0.1", 8100),
                new HostPort("127.0.0.1", 8101), new HostPort("127.0.0.1", 16479),
                new HostPort("127.0.0.1", 17479));
        manager.visitAdded(getDcMeta("oy").findCluster("cluster1"));
        verify(instanceManager, times(4)).getOrCreate(any(RedisMeta.class));
        Assert.assertEquals(expectedRedises, addedRedises);
    }

    @Test
    public void visitAddBiDirectionCluster() {
        // add cluster not in current dc
        manager.visitAdded(getDcMeta("oy").findCluster("cluster4"));
        verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));

        // add cluster in current dc
        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("10.0.0.1", 6379),
                new HostPort("10.0.0.2", 6379));
        manager.visitAdded(getDcMeta("jq").findCluster("cluster3"));
        manager.visitAdded(getDcMeta("oy").findCluster("cluster3"));

        verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
        Assert.assertEquals(expectedRedises, addedRedises);
    }

    @Test
    public void visitModified() {
        ClusterMeta clusterMeta = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta clone = MetaClone.clone(clusterMeta);
        clone.getShards().get("shard2").addRedis(new RedisMeta());
        manager.visitModified(new ClusterMetaComparator(clusterMeta, clone));
        verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
    }

    @Test
    public void testMasterChange() throws Exception {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster1").getShards().values().iterator().next().getRedises().get(0).setMaster("");
        manager.compare(future);

        // only changed redis reload
        Mockito.verify(instanceManager, times(1)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, times(1)).getOrCreate(any(RedisMeta.class));
    }

    @Test
    public void testShardConfigChange() throws Exception {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster1").getShards().values().iterator().next().setSentinelId(100L);
        manager.compare(future);

        // only redis in changed shard reload
        Mockito.verify(instanceManager, times(2)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
    }

    @Test
    public void testActiveDcInterestedNotChange() {
        // active dc is always not current dc jq
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setActiveDc("rb");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));

        // active dc is always current dc jq
        cluster = getDcMeta("oy").findCluster("cluster1");
        newCluster = MetaClone.clone(cluster);
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));
        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
    }

    @Test
    public void testActiveDcOY2JQ() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster2").setActiveDc("jq");
        manager.compare(future);

        Mockito.verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.2", 8100), new HostPort("127.0.0.2", 8101)), addedRedises);
    }

    @Test
    public void testActiveDcJQ2OY() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster1").setActiveDc("oy");
        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(4)).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.1", 8100), new HostPort("127.0.0.1", 8101),
                 new HostPort("127.0.0.1", 16479), new HostPort("127.0.0.1", 17479)), deletedRedised);
    }

    @Test
    public void testDcsInterestedNotChange() {
        // current dc is always not in dcs
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster4");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("rb");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));

        // current dc is always in dcs
        cluster = getDcMeta("oy").findCluster("cluster3");
        newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("jq,oy,rb");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
    }

    @Test
    public void testDcsAddCurrentDc() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster4").setDcs("jq,oy");
        manager.compare(future);

        Mockito.verify(instanceManager).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager).remove(any(HostPort.class)); // delete anyway
        Assert.assertEquals(Sets.newHashSet(new HostPort("10.0.0.2", 6479)), addedRedises);
    }

    @Test
    public void testDcsDeleteCurrentDc() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster3").setDcs("oy");
        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(1)).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("10.0.0.2", 6379)), deletedRedised);
    }

    @Test
    public void testClusterOrgChange() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster3").setOrgId(2);
        manager.compare(future);

        Mockito.verify(instanceManager).remove(any(HostPort.class));
        Mockito.verify(instanceManager).getOrCreate(any(RedisMeta.class));
    }

    @Test
    public void visitRemoved() {
        manager = spy(manager);
        manager.compare(getDcMeta("oy"));
        verify(manager, never()).visitModified(any());
        verify(manager, never()).visitAdded(any());
        verify(manager, never()).visitRemoved(any());

        DcMeta dcMeta = MetaClone.clone(getDcMeta("oy"));

        ClusterMeta clusterMeta = dcMeta.getClusters().remove("cluster1");
        clusterMeta.setId("cluster5").setDbId(Math.abs(randomLong())).getShards().values().forEach(shardMeta -> {
            shardMeta.setParent(clusterMeta);
            for (RedisMeta redis : shardMeta.getRedises()) {
                redis.setParent(shardMeta);
            }
        });
        dcMeta.addCluster(clusterMeta);
        manager.compare(dcMeta);
        verify(manager, atLeastOnce()).visitRemoved(any());
        verify(manager, atLeastOnce()).visitAdded(any());
        verify(manager, never()).visitModified(any());
        verify(instanceManager, never()).remove("cluster1");
    }

    @Test
    public void visitRemovedClusterActiveDc() {
        manager = spy(new DefaultDcMetaChangeManager("jq", instanceManager, factory));
        manager.compare(getDcMeta("jq"));

        DcMeta dcMeta = MetaClone.clone(getDcMeta("jq"));
        dcMeta.getClusters().remove("cluster1");

        manager.compare(dcMeta);
        verify(manager, atLeastOnce()).visitRemoved(any());
        verify(manager, never()).visitAdded(any());
        verify(manager, never()).visitModified(any());
        verify(instanceManager, times(1)).remove("cluster1");
    }

    private ClusterHealthCheckInstance mockClusterHealthCheckInstance(String clusterId, String activeDc, ClusterType clusterType, int orgId) {
        DefaultClusterHealthCheckInstance instance = new DefaultClusterHealthCheckInstance();
        DefaultClusterInstanceInfo info = new DefaultClusterInstanceInfo(clusterId, activeDc, clusterType, orgId);
        instance.setInstanceInfo(info);
        return instance;
    }

    @Test
    public void testRouteChange() {
        Mockito.doNothing().when(factory).updateRoutes();
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.getRoutes().get(0).setIsPublic(false);
        manager.compare(future);
        Mockito.verify(factory, times(2)).updateRoutes();

    }


    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}