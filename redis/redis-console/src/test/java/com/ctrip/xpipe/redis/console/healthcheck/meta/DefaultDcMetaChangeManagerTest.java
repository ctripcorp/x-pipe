package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
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
        }).when(instanceManager).getOrCreate(any());
        Mockito.doAnswer(invocation -> {
            HostPort redis = invocation.getArgumentAt(0, HostPort.class);
            deletedRedised.add(redis);
            return null;
        }).when(instanceManager).remove(any());

        manager = new DefaultDcMetaChangeManager(instanceManager);
    }

    @Test
    public void compare() {

    }

    @Test
    public void visitAddOneWayCluster() {
        manager.visitAdded(getDcMeta("oy").findCluster("cluster2"));
        verify(instanceManager, never()).getOrCreate(any());

        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("127.0.0.1", 8100),
                new HostPort("127.0.0.1", 8101));
        manager.visitAdded(getDcMeta("oy").findCluster("cluster1"));
        verify(instanceManager, times(2)).getOrCreate(any());
        Assert.assertEquals(expectedRedises, addedRedises);
    }

    @Test
    public void visitAddBiDirectionCluster() {
        // add cluster not in current dc
        manager.visitAdded(getDcMeta("oy").findCluster("cluster4"));
        verify(instanceManager, never()).getOrCreate(any());

        // add cluster in current dc
        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("10.0.0.1", 6379),
                new HostPort("10.0.0.2", 6379));
        manager.visitAdded(getDcMeta("jq").findCluster("cluster3"));
        manager.visitAdded(getDcMeta("oy").findCluster("cluster3"));

        verify(instanceManager, times(2)).getOrCreate(any());
        Assert.assertEquals(expectedRedises, addedRedises);
    }

    @Test
    public void visitModified() {
        ClusterMeta clusterMeta = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta clone = MetaClone.clone(clusterMeta);
        clone.getShards().get("shard2").addRedis(new RedisMeta());
        manager.visitModified(new ClusterMetaComparator(clusterMeta, clone));
        verify(instanceManager, never()).getOrCreate(any());
    }

    @Test
    public void testMasterChange() throws Exception {
        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = mock(RedisInstanceInfo.class);
        when(instance.getRedisInstanceInfo()).thenReturn(info);

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

        Mockito.verify(instanceManager, never()).getOrCreate(any());
        Mockito.verify(instanceManager, never()).remove(any());

        // active dc is always current dc jq
        cluster = getDcMeta("oy").findCluster("cluster1");
        newCluster = MetaClone.clone(cluster);
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));
        Mockito.verify(instanceManager, never()).getOrCreate(any());
        Mockito.verify(instanceManager, never()).remove(any());
    }

    @Test
    public void testActiveDcOY2JQ() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setActiveDc("jq");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, times(2)).getOrCreate(any());
        Mockito.verify(instanceManager, never()).remove(any());
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.2", 8100), new HostPort("127.0.0.2", 8101)), addedRedises);
    }

    @Test
    public void testActiveDcJQ2OY() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster1");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setActiveDc("oy");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any());
        Mockito.verify(instanceManager, times(2)).remove(any());
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.1", 8100), new HostPort("127.0.0.1", 8101)), deletedRedised);
    }

    @Test
    public void testDcsInterestedNotChange() {
        // current dc is always not in dcs
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster4");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("rb");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any());
        Mockito.verify(instanceManager, never()).remove(any());

        // current dc is always in dcs
        cluster = getDcMeta("oy").findCluster("cluster3");
        newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("jq,oy,rb");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any());
        Mockito.verify(instanceManager, never()).remove(any());
    }

    @Test
    public void testDcsAddCurrentDc() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster4");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("jq,oy");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, times(1)).getOrCreate(any());
        Mockito.verify(instanceManager, never()).remove(any());
        Assert.assertEquals(Sets.newHashSet(new HostPort("10.0.0.2", 6479)), addedRedises);
    }

    @Test
    public void testDcsDeleteCurrentDc() {
        ClusterMeta cluster = getDcMeta("oy").findCluster("cluster3");
        ClusterMeta newCluster = MetaClone.clone(cluster);
        newCluster.setDcs("oy");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any());
        Mockito.verify(instanceManager, times(1)).remove(any());
        Assert.assertEquals(Sets.newHashSet(new HostPort("10.0.0.2", 6379)), deletedRedised);
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
    }



    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}