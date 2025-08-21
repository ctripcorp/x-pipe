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
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
    public void beforeDefaultDcMetaChangeManagerTest() throws IOException, SAXException {
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
        Mockito.doAnswer(invocation -> {
            HostPort redis = invocation.getArgument(0, HostPort.class);
            deletedRedised.add(redis);
            return null;
        }).when(instanceManager).removeRedisInstanceForPingAction(any(HostPort.class));

        when(checkerConfig.getConsoleAddress()).thenReturn("127.0.0.1");
        when(metaCache.isCrossRegion("jq", "fra-aws")).thenReturn(true);
        
        manager = new DefaultDcMetaChangeManager("oy", instanceManager, factory, metaCache);
    }

    private void prepareData(String dc) {
        manager.compare(getDcMeta(dc));
    }

    private DcMeta cloneDcMeta(String dc) {
        return cloneDcMeta(getDcMeta(dc));
    }

    private DcMeta cloneDcMeta(DcMeta source){
        DcMeta dcMeta = MetaCloneFacade.INSTANCE.clone(source);
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
    public void visitModified() {
        ClusterMeta clusterMeta = getDcMeta("oy").findCluster("cluster2");
        ClusterMeta clone = MetaCloneFacade.INSTANCE.clone(clusterMeta);
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
        ClusterMeta newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        newCluster.setActiveDc("rb");
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));

        // active dc is always current dc jq
        cluster = getDcMeta("oy").findCluster("cluster1");
        newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        manager.visitModified(new ClusterMetaComparator(cluster, newCluster));
        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
    }

    @Test
    public void testActiveDcOY2JQ() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster2").setActiveDc("jq").setBackupDcs("oy");
        manager.compare(future);

        Mockito.verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.2", 8100), new HostPort("127.0.0.2", 8101)), addedRedises);
    }

    @Test
    public void testActiveDcJQ2OY() {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        future.findCluster("cluster1").setActiveDc("oy").setBackupDcs("jq");
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
        ClusterMeta newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        newCluster.setDcs("rb");
        ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(cluster, newCluster);
        clusterMetaComparator.compare();
        manager.visitModified(clusterMetaComparator);

        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));

        // current dc is always in dcs
        cluster = getDcMeta("oy").findCluster("cluster3");
        newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        newCluster.setDcs("jq,oy,rb");
        ClusterMetaComparator clusterMetaComparator1 = new ClusterMetaComparator(cluster, newCluster);
        clusterMetaComparator1.compare();
        manager.visitModified(clusterMetaComparator1);

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

        DcMeta dcMeta = MetaCloneFacade.INSTANCE.clone(getDcMeta("oy"));

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
    public void visitRemovedClusterActiveDc(){
        manager = spy(new DefaultDcMetaChangeManager("jq", instanceManager, factory, metaCache));
        manager.compare(getDcMeta("jq"));

        DcMeta dcMeta = MetaCloneFacade.INSTANCE.clone(getDcMeta("jq"));
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
        Mockito.verify(factory, times(1)).updateRoutes();
        DcMeta future = cloneDcMeta("oy");
        future.getRoutes().get(0).setIsPublic(false);
        manager.compare(future);
        Mockito.verify(factory, times(2)).updateRoutes();

    }

    @Test
    public void testSwitchClusterName() throws Exception {
        prepareData("oy");
        DcMeta future = cloneDcMeta("oy");
        ClusterMeta cluster1 = future.findCluster("cluster1");
        ClusterMeta cluster2 = future.findCluster("cluster2");

        future.addCluster(cluster1.setId("cluster2"));
        future.addCluster(cluster2.setId("cluster1"));
        manager.compare(future);

        Mockito.verify(instanceManager, times(1)).getOrCreate(any(ClusterMeta.class));
        Mockito.verify(instanceManager, times(4)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(4)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, never()).remove(anyString());
    }

    private void changeClusterShardId(ClusterMeta clusterMeta) {
        Map<String, ShardMeta> modified = new HashMap<>();
        for (ShardMeta value : clusterMeta.getShards().values()) {
            String id = value.getId();
            String newId = clusterMeta.getId() + id;
            modified.put(newId, value.setId(newId));
        }
        changeClusterShards(clusterMeta, modified);
    }

    private void changeClusterShards(ClusterMeta clusterMeta, Map<String, ShardMeta> shards) {
        clusterMeta.getShards().clear();
        for (ShardMeta value : shards.values()) {
            clusterMeta.addShard(value);
        }
    }

    @Test
    public void testSwitchClusterShards() throws Exception {
       DcMeta dcMeta= getDcMeta("oy");
        ClusterMeta cluster1 = dcMeta.findCluster("cluster1");
        changeClusterShardId(cluster1);

        ClusterMeta cluster2 = dcMeta.findCluster("cluster2");
        changeClusterShardId(cluster2);

        manager.compare(dcMeta);

        DcMeta future = cloneDcMeta(dcMeta);
        ClusterMeta cluster1Future = future.findCluster("cluster1");
        Map<String, ShardMeta> cluster1ShardsCopy = new HashMap<>(cluster1Future.getShards());
        ClusterMeta cluster2Future = future.findCluster("cluster2");
        Map<String, ShardMeta> cluster2ShardsCopy = new HashMap<>(cluster2Future.getShards());

        changeClusterShards(cluster1Future,cluster2ShardsCopy);
        changeClusterShards(cluster2Future,cluster1ShardsCopy);

        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreate(any(ClusterMeta.class));
        Mockito.verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(6)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, never()).remove(anyString());
    }

    @Test
    public void testHeteroClusterModified() throws Exception {
        DefaultDcMetaChangeManager  manager = new DefaultDcMetaChangeManager("jq", instanceManager, factory, metaCache);
        DcMeta dcMeta= getDcMeta("jq");
        ClusterMeta cluster1 = dcMeta.findCluster("cluster2");
        cluster1.setBackupDcs("ali");
        cluster1.setAzGroupType(ClusterType.ONE_WAY.toString());
        manager.compare(dcMeta);


        DcMeta future = cloneDcMeta(dcMeta);
        ClusterMeta futureCluster = future.findCluster("cluster2");
        futureCluster.setBackupDcs("ali");
        futureCluster.setAzGroupType(ClusterType.SINGLE_DC.toString());

        manager.compare(future);

        Mockito.verify(instanceManager, times(1)).getOrCreate(any(ClusterMeta.class));
        Mockito.verify(instanceManager, times(2)).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, times(1)).remove(anyString());

    }


    @Test
    public void testBackupDcClusterModified() throws Exception {
        DefaultDcMetaChangeManager  manager = new DefaultDcMetaChangeManager("jq", instanceManager, factory, metaCache);
        DcMeta dcMeta= getDcMeta("jq");
        ClusterMeta cluster1 = dcMeta.findCluster("cluster2");
        cluster1.setBackupDcs("jq,ali");
        manager.compare(dcMeta);


        DcMeta future = cloneDcMeta(dcMeta);
        ClusterMeta futureCluster = future.findCluster("cluster2");
        futureCluster.setBackupDcs("jq");

        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreate(any(ClusterMeta.class));
        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).remove(any(HostPort.class));
        Mockito.verify(instanceManager, times(1)).remove(anyString());

    }

    @Test
    public void generateCRossDCHealthCheckInstancesTest() throws Exception {
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("cross_dc_cluster").setType("CROSS_DC").setActiveDc("oy").setDcs("jq,oy").setParent(new DcMeta("jq"));
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("cross_dc_cluster").setType("CROSS_DC").setActiveDc("oy").setDcs("jq,oy").setParent(new DcMeta("oy"));

        Assert.assertFalse(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertFalse(manager.isInterestedInCluster(oyClusterMeta));

        jqClusterMeta.setActiveDc("jq");
        oyClusterMeta.setActiveDc("jq");
        Assert.assertTrue(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertTrue(manager.isInterestedInCluster(oyClusterMeta));

    }

    @Test
    public void generateSingleDcHealthCheckInstancesTest() throws Exception {
        ClusterMeta clusterMeta = new ClusterMeta().setId("single_dc_cluster").setType("SINGLE_DC").setActiveDc("jq").setParent(new DcMeta("jq"));
        Assert.assertTrue(manager.isInterestedInCluster(clusterMeta));

        clusterMeta.setActiveDc("oy").setAzGroupName("oy");
        Assert.assertFalse(manager.isInterestedInCluster(clusterMeta));
    }

    @Test
    public void generateLocalDCHealthCheckInstancesTest() throws Exception {
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("local_dc_cluster").setType("LOCAL_DC").setDcs("jq,oy").setParent(new DcMeta("jq"));
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("local_dc_cluster").setType("LOCAL_DC").setDcs("jq,oy").setParent(new DcMeta("oy"));

        Assert.assertTrue(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertTrue(manager.isInterestedInCluster(oyClusterMeta));
    }

    @Test
    public void generateOneWayHealthCheckInstancesTest() throws Exception {
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setParent(new DcMeta("jq")).setAzGroupType("ONE_WAY");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setParent(new DcMeta("oy")).setAzGroupType("ONE_WAY");
        Assert.assertTrue(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertTrue(manager.isInterestedInCluster(oyClusterMeta));

        jqClusterMeta.setActiveDc("oy");
        oyClusterMeta.setActiveDc("oy");
        Assert.assertFalse(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertFalse(manager.isInterestedInCluster(oyClusterMeta));
    }

    @Test
    public void generateHeteroHealthCheckInstancesTest() throws Exception {
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("hetero_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setParent(new DcMeta("jq")).setAzGroupType("ONE_WAY");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("hetero_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setParent(new DcMeta("oy")).setAzGroupType("ONE_WAY");
        ClusterMeta awsClusterMeta = new ClusterMeta().setId("hetero_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setParent(new DcMeta("aws")).setAzGroupType("SINGLE_DC");


        // current dc is active dc
        Assert.assertTrue(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertTrue(manager.isInterestedInCluster(oyClusterMeta));
        Assert.assertTrue(manager.isInterestedInCluster(awsClusterMeta));

        //current dc is not active dc but in dr master type
        jqClusterMeta.setActiveDc("oy");
        oyClusterMeta.setActiveDc("oy");
        awsClusterMeta.setActiveDc("oy");
        Assert.assertFalse(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertFalse(manager.isInterestedInCluster(oyClusterMeta));
        Assert.assertFalse(manager.isInterestedInCluster(awsClusterMeta));

        //current dc is in master type
        jqClusterMeta.setAzGroupType("SINGLE_DC");
        oyClusterMeta.setAzGroupType("ONE_WAY");
        awsClusterMeta.setAzGroupType("ONE_WAY");
        Assert.assertTrue(manager.isInterestedInCluster(jqClusterMeta));
        Assert.assertFalse(manager.isInterestedInCluster(oyClusterMeta));
        Assert.assertFalse(manager.isInterestedInCluster(awsClusterMeta));
    }

    @Test
    public void visitCrossRegionModified() {
        ClusterMeta clusterMeta = getDcMeta("fra-aws").findCluster("cluster2");
        ClusterMeta clone = MetaCloneFacade.INSTANCE.clone(clusterMeta);
        clone.getShards().get("shard2").addRedis(new RedisMeta());
        manager.visitModified(new ClusterMetaComparator(clusterMeta, clone));
        verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
    }

    @Test
    public void testCrossRegionShardConfigChange() throws Exception {
        prepareData("jq");
        DcMeta future = cloneDcMeta("jq");
        future.findCluster("cluster5").getShards().values().iterator().next().setSentinelId(100L);
        manager.compare(future);

        // only redis in changed shard reload
        Mockito.verify(instanceManager, times(2)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));
        Mockito.verify(instanceManager, times(2)).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
    }

    @Test
    public void testCrossRegionActiveDcFraAws2FraAli() {
        prepareData("fra-ali");
        DcMeta future = cloneDcMeta("fra-ali");
        future.findCluster("cluster5").setActiveDc("fra-aws").setBackupDcs("fra-ali");
        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.4", 8200), new HostPort("127.0.0.4", 8201)), deletedRedised);
    }

    @Test
    public void testCrossRegionActiveDcFraAli2FraAws() {
        prepareData("fra-aws");
        DcMeta future = cloneDcMeta("fra-aws");
        future.findCluster("cluster5").setActiveDc("fra-ali").setBackupDcs("fra-aws");
        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.3", 8200), new HostPort("127.0.0.3", 8201)), deletedRedised);
    }

    @Test
    public void testCrossRegionDcsInterestedNotChange() {
        // current dc is always not in dcs
        ClusterMeta cluster = getDcMeta("fra-aws").findCluster("cluster5");
        ClusterMeta newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        newCluster.setDcs("rb");
        ClusterMetaComparator clusterMetaComparator = new ClusterMetaComparator(cluster, newCluster);
        clusterMetaComparator.compare();
        manager.visitModified(clusterMetaComparator);

        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).removeRedisInstanceForPingAction(any(HostPort.class));

        // current dc is always in dcs
        cluster = getDcMeta("fra-aws").findCluster("cluster5");
        newCluster = MetaCloneFacade.INSTANCE.clone(cluster);
        newCluster.setDcs("fra-aws, fra-ali");
        ClusterMetaComparator clusterMetaComparator1 = new ClusterMetaComparator(cluster, newCluster);
        clusterMetaComparator1.compare();
        manager.visitModified(clusterMetaComparator1);

        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
        Mockito.verify(instanceManager, never()).removeRedisInstanceForPingAction(any(HostPort.class));
    }

    @Test
    public void testCrossRegionDcsAddCurrentDc() {
        prepareData("fra-aws");
        DcMeta future = cloneDcMeta("fra-aws");
        future.findCluster("cluster5").setDcs("fra-aws, fra-ali");
        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class)); // delete anyway
    }

    @Test
    public void testCrossRegionDcsDeleteCurrentDc() {
        prepareData("fra-aws");
        DcMeta future = cloneDcMeta("fra-aws");
        future.findCluster("cluster5").setDcs("fra-aws");
        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));
        Assert.assertEquals(Sets.newHashSet(new HostPort("127.0.0.3", 8200), new HostPort("127.0.0.3", 8201)), deletedRedised);
    }

    @Test
    public void testCrossRegionClusterOrgChange() {
        prepareData("fra-aws");
        DcMeta future = cloneDcMeta("fra-aws");
        future.findCluster("cluster5").setOrgId(2);
        manager.compare(future);

        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));
        Mockito.verify(instanceManager, never()).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));
    }

    @Test
    public void visitCrossRegionRemoved() {
        manager = spy(manager);
        manager.compare(getDcMeta("fra-aws"));
        verify(manager, never()).visitModified(any());
        verify(manager, never()).visitAdded(any());
        verify(manager, never()).visitRemoved(any());

        DcMeta dcMeta = MetaCloneFacade.INSTANCE.clone(getDcMeta("fra-aws"));

        ClusterMeta clusterMeta = dcMeta.getClusters().remove("cluster5");
        clusterMeta.setId("cluster6").setDbId(Math.abs(randomLong())).getShards().values().forEach(shardMeta -> {
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
        verify(instanceManager, never()).remove("cluster5");
    }

    @Test
    public void visitCrossRegionRemovedClusterActiveDc(){
        manager = spy(new DefaultDcMetaChangeManager("fra-aws", instanceManager, factory, metaCache));
        manager.compare(getDcMeta("fra-aws"));

        DcMeta dcMeta = MetaCloneFacade.INSTANCE.clone(getDcMeta("fra-aws"));
        dcMeta.getClusters().remove("cluster5");

        manager.compare(dcMeta);
        verify(manager, atLeastOnce()).visitRemoved(any());
        verify(manager, never()).visitAdded(any());
        verify(manager, never()).visitModified(any());
        verify(instanceManager, never()).remove("cluster5");
        verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));
    }

    @Test
    public void testCrossRegionSwitchClusterName() throws Exception {
        prepareData("fra-aws");
        DcMeta future = cloneDcMeta("fra-aws");
        ClusterMeta cluster5 = future.findCluster("cluster5");
        ClusterMeta cluster6 = future.findCluster("cluster6");

        future.addCluster(cluster5.setId("cluster6"));
        future.addCluster(cluster6.setId("cluster5"));
        manager.compare(future);

        Mockito.verify(instanceManager, times(4)).removeRedisInstanceForPingAction(any(HostPort.class));
        Mockito.verify(instanceManager, never()).remove(anyString());
    }

    @Test
    public void testCrossRegionSwitchClusterShards() throws Exception {
        DcMeta dcMeta= getDcMeta("fra-aws");
        ClusterMeta cluster5 = dcMeta.findCluster("cluster5");
        changeClusterShardId(cluster5);

        ClusterMeta cluster6 = dcMeta.findCluster("cluster6");
        changeClusterShardId(cluster6);

        manager.compare(dcMeta);

        DcMeta future = cloneDcMeta(dcMeta);
        ClusterMeta cluster5Future = future.findCluster("cluster5");
        Map<String, ShardMeta> cluster1ShardsCopy = new HashMap<>(cluster5Future.getShards());
        ClusterMeta cluster6Future = future.findCluster("cluster6");
        Map<String, ShardMeta> cluster2ShardsCopy = new HashMap<>(cluster6Future.getShards());

        changeClusterShards(cluster5Future,cluster2ShardsCopy);
        changeClusterShards(cluster6Future,cluster1ShardsCopy);

        manager.compare(future);

        Mockito.verify(instanceManager, times(4)).removeRedisInstanceForPingAction(any(HostPort.class));
        Mockito.verify(instanceManager, never()).remove(anyString());
    }

    @Test
    public void testCrossRegionHeteroClusterModified() throws Exception {
        DefaultDcMetaChangeManager  manager = new DefaultDcMetaChangeManager("fra-aws", instanceManager, factory, metaCache);
        DcMeta dcMeta= getDcMeta("fra-aws");
        ClusterMeta cluster1 = dcMeta.findCluster("cluster5");
        cluster1.setBackupDcs("ali");
        cluster1.setAzGroupType(ClusterType.ONE_WAY.toString());
        manager.compare(dcMeta);


        DcMeta future = cloneDcMeta(dcMeta);
        ClusterMeta futureCluster = future.findCluster("cluster5");
        futureCluster.setBackupDcs("ali");
        futureCluster.setAzGroupType(ClusterType.SINGLE_DC.toString());

        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreate(any(ClusterMeta.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));

    }

    @Test
    public void testCrossRegionBackupDcClusterModified() throws Exception {
        DefaultDcMetaChangeManager  manager = new DefaultDcMetaChangeManager("fra-aws", instanceManager, factory, metaCache);
        DcMeta dcMeta= getDcMeta("fra-aws");
        ClusterMeta cluster5 = dcMeta.findCluster("cluster5");
        cluster5.setBackupDcs("fra-aws,fra-ali");
        manager.compare(dcMeta);


        DcMeta future = cloneDcMeta(dcMeta);
        ClusterMeta futureCluster = future.findCluster("cluster5");
        futureCluster.setBackupDcs("fra-aws");

        manager.compare(future);

        Mockito.verify(instanceManager, never()).getOrCreate(any(ClusterMeta.class));
        Mockito.verify(instanceManager, never()).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instanceManager, times(2)).remove(any(HostPort.class));
        Mockito.verify(instanceManager, times(2)).removeRedisInstanceForPingAction(any(HostPort.class));

    }

    protected DcMeta getDcMeta(String dc) {
        Map<String, DcMeta> dcMetaMap = getXpipeMeta().getDcs();
        DcMeta dcMeta = dcMetaMap.get(dc);
        dcMeta.getClusters().values().forEach(clusterMeta -> {
            if (ClusterType.lookup(clusterMeta.getType()).supportSingleActiveDC()) {
                clusterMeta.setBackupDcs(getBackupDcs(dcMetaMap, clusterMeta));
            }
        });

        return dcMeta;
    }

    protected String getBackupDcs(Map<String, DcMeta> dcMetaMap, ClusterMeta clusterMeta) {
        StringBuilder sb = new StringBuilder();
        for (String dc : dcMetaMap.keySet()) {
            if (dcMetaMap.get(dc).findCluster(clusterMeta.getId()) != null) {
                if (!dc.equalsIgnoreCase(clusterMeta.getActiveDc())) {
                    sb.append(dc).append(",");
                }
            }
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }
}