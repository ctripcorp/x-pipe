package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
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

    private RedisHealthCheckInstance instance = null;

    private Set<HostPort> addedRedises = new HashSet<>();

    private Set<HostPort> deletedRedised = new HashSet<>();

    @Before
    public void beforeDefaultDcMetaChangeManagerTest() {
        MockitoAnnotations.initMocks(this);
        Mockito.doAnswer(invocation -> {
            RedisMeta redis = invocation.getArgumentAt(0, RedisMeta.class);
            HostPort redisHostPort = new HostPort(redis.getIp(), redis.getPort());
            addedRedises.add(redisHostPort);
            return instance;
        }).when(instanceManager).getOrCreate(any(RedisMeta.class));
        Mockito.doAnswer(invocation -> {
            HostPort redis = invocation.getArgumentAt(0, HostPort.class);
            deletedRedised.add(redis);
            return null;
        }).when(instanceManager).remove(any(HostPort.class));
        
        manager = new DefaultDcMetaChangeManager("oy", instanceManager, factory);
    }

    @Test
    public void compare() {

    }

    @Test
    public void visitAddOneWayCluster() {
        manager.visitAdded(getDcMeta("oy").findCluster("cluster2"));
        verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));

        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("127.0.0.1", 8100),
                new HostPort("127.0.0.1", 8101));
        manager.visitAdded(getDcMeta("oy").findCluster("cluster1"));
        verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
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
        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = mock(RedisInstanceInfo.class);
        when(instance.getCheckInfo()).thenReturn(info);

        // become master
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster1");
        XpipeMeta newXpipeMeta = loadXpipeMeta(getXpipeMetaConfigFile());
        ClusterMeta newCluster = newXpipeMeta.getDcs().get("oy").findCluster("cluster1");
        newCluster.getShards().values().iterator().next().getRedises().get(0).setMaster("");
        ClusterMetaComparator comparator = new ClusterMetaComparator(cluster, newCluster);
        comparator.compare();
        manager.visitModified(comparator);

        verify(info, times(1)).isMaster(anyBoolean());
        verify(info, times(1)).isMaster(true);

        // lose master
        cluster = newCluster;
        newXpipeMeta = loadXpipeMeta(getXpipeMetaConfigFile());
        newCluster = newXpipeMeta.getDcs().get("oy").findCluster("cluster1");
        newCluster.getShards().values().iterator().next().getRedises().get(0).setMaster("127.0.0.1:6100");
        comparator = new ClusterMetaComparator(cluster, newCluster);
        comparator.compare();
        manager.visitModified(comparator);

        verify(info, times(2)).isMaster(anyBoolean());
        verify(info, times(1)).isMaster(false);
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
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setActiveDc("jq");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.2", 8100), new HostPort("127.0.0.2", 8101)), addedRedises);
    }

    @Test
    public void testActiveDcJQ2OY() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster1");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setActiveDc("oy");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.1", 8100), new HostPort("127.0.0.1", 8101)), deletedRedised);
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
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster4");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("jq,oy");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, times(1)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("10.0.0.2", 6479)), addedRedises);
    }

    @Test
    public void testDcsDeleteCurrentDc() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster3");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("oy");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(1)).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("10.0.0.2", 6379)), deletedRedised);
    }

    @Test
    public void testClusterOrgChange() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster3");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        cluster.setOrgId(1);
        newCluster.setOrgId(2);

        ClusterHealthCheckInstance instance = mockClusterHealthCheckInstance(cluster.getId(), cluster.getActiveDc(), ClusterType.lookup(cluster.getType()), 1);
        Mockito.when(instanceManager.findClusterHealthCheckInstance(Mockito.anyString())).thenReturn(instance);

        Assert.assertEquals(1, instance.getCheckInfo().getOrgId());
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));
        Assert.assertEquals(2, instance.getCheckInfo().getOrgId());
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
        clusterMeta.setId("cluster5").getShards().values().forEach(shardMeta -> {
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



    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}