package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityRefreshManager;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.MetaChangeManager;
import com.ctrip.xpipe.redis.core.entity.*;
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
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

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
    private KeeperCapabilityRefreshManager keeperCapabilityRefreshManager;

    @Mock
    private CheckerConsoleService checkerConsoleService;

    @Mock
    private KeeperCheckSelector keeperSelector;

    @Before
    public void setupDefaultHealthCheckerMockTest() throws IOException, SAXException {
        when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
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
    public void generateCRossDCHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("cross_dc_cluster").setType("CROSS_DC").setActiveDc("oy").setDcs("jq,oy").setAzGroupName("jq");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("cross_dc_cluster").setType("CROSS_DC").setActiveDc("oy").setDcs("jq,oy").setAzGroupName("oy");
        oyDcMeta.addCluster(oyClusterMeta);
        xpipeMeta.addDc(jqDcMeta).addDc(oyDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        checker.generateHealthCheckInstances();
        verify(instanceManager, never()).getOrCreate(new ClusterMeta().setId("cross_dc_cluster"));

        jqClusterMeta.setActiveDc("jq");
        oyClusterMeta.setActiveDc("jq");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(2)).getOrCreate(new ClusterMeta().setId("cross_dc_cluster"));
    }

    @Test
    public void generateSingleDcHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta clusterMeta = new ClusterMeta().setId("single_dc_cluster").setType("SINGLE_DC").setActiveDc("jq").setAzGroupName("jq");
        jqDcMeta.addCluster(clusterMeta);
        xpipeMeta.addDc(jqDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        checker.generateHealthCheckInstances();
        verify(instanceManager, times(1)).getOrCreate(new ClusterMeta().setId("single_dc_cluster"));

        clusterMeta.setActiveDc("oy").setAzGroupName("oy");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(1)).getOrCreate(new ClusterMeta().setId("single_dc_cluster"));
    }

    @Test
    public void generateLocalDCHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("local_dc_cluster").setType("LOCAL_DC").setDcs("jq,oy").setAzGroupName("jq");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("local_dc_cluster").setType("LOCAL_DC").setDcs("jq,oy").setAzGroupName("oy");
        oyDcMeta.addCluster(oyClusterMeta);
        xpipeMeta.addDc(jqDcMeta).addDc(oyDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        checker.generateHealthCheckInstances();
        verify(instanceManager, times(2)).getOrCreate(new ClusterMeta().setId("local_dc_cluster"));
    }

    @Test
    public void generateOneWayHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setAzGroupName("jq").setAzGroupType("ONE_WAY");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setAzGroupName("oy").setAzGroupType("ONE_WAY");
        oyDcMeta.addCluster(oyClusterMeta);
        xpipeMeta.addDc(jqDcMeta).addDc(oyDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        checker.generateHealthCheckInstances();
        verify(instanceManager, times(2)).getOrCreate(new ClusterMeta().setId("one_way_cluster"));

        jqClusterMeta.setActiveDc("oy");
        oyClusterMeta.setActiveDc("oy");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(2)).getOrCreate(new ClusterMeta().setId("one_way_cluster"));
    }

    @Test
    public void generateCrossRegionHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq").setZone("SHA");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("fra").setBackupDcs("jq").setAzGroupName("jq").setAzGroupType("ONE_WAY");
        jqDcMeta.addCluster(jqClusterMeta);
        RedisMeta jqRedisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379);
        jqClusterMeta.addShard(new ShardMeta().setId("one_way_shard").addRedis(jqRedisMeta));

        DcMeta fraDcMeta = new DcMeta("fra").setZone("FRA");

        xpipeMeta.addDc(jqDcMeta).addDc(fraDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        Mockito.doAnswer(inv -> {
            String currentDc = inv.getArgument(0, String.class);
            String otherDc = inv.getArgument(1, String.class);
            DcMeta currentDcMeta = xpipeMeta.findDc(currentDc);
            DcMeta otherDcMeta = xpipeMeta.findDc(otherDc);
            if (null == currentDcMeta || null == otherDcMeta) return false;
            return !currentDcMeta.getZone().equalsIgnoreCase(otherDcMeta.getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        checker.generateHealthCheckInstances();
        verify(instanceManager, times(1)).getOrCreate(new ClusterMeta().setId("one_way_cluster"));
        verify(instanceManager, times(1)).getOrCreateRedisInstanceForPsubPingAction(any(RedisMeta.class));

        jqClusterMeta.setActiveDc("jq");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(2)).getOrCreate(new ClusterMeta().setId("one_way_cluster"));
        verify(instanceManager, times(1)).getOrCreate(any(RedisMeta.class));
    }

    @Test
    public void testGenerateKeeperHealthCheckInstancesUsesSelector() {
        XpipeMeta meta = new XpipeMeta();
        DcMeta dc = new DcMeta("jq");
        ClusterMeta cluster = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq");
        dc.addCluster(cluster);
        meta.addDc(dc);
        KeeperMeta keeper = new KeeperMeta().setIp("127.0.0.9").setPort(6380);
        when(metaCache.getXpipeMeta()).thenReturn(meta);
        when(keeperSelector.select(cluster)).thenReturn(Collections.singletonList(keeper));

        checker.generateHealthCheckInstances();

        verify(instanceManager).getOrCreate(keeper);
    }

    @Test
    public void testKeeperManagerLifecycleOrder() throws Exception {
        XpipeMeta meta = new XpipeMeta();
        DcMeta dc = new DcMeta("jq");
        ClusterMeta cluster = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq");
        dc.addCluster(cluster);
        meta.addDc(dc);
        KeeperMeta keeper = new KeeperMeta().setIp("127.0.0.9").setPort(6380);
        when(metaCache.getXpipeMeta()).thenReturn(meta);
        when(keeperSelector.select(cluster)).thenReturn(Collections.singletonList(keeper));

        checker.doInitialize();
        checker.doStart();
        checker.doStop();

        org.mockito.InOrder lifecycle = inOrder(instanceManager, metaChangeManager, keeperCapabilityRefreshManager);
        lifecycle.verify(instanceManager).getOrCreate(keeper);
        lifecycle.verify(metaChangeManager).start();
        lifecycle.verify(keeperCapabilityRefreshManager).start();
        lifecycle.verify(keeperCapabilityRefreshManager).stop();
        lifecycle.verify(metaChangeManager).stop();
    }

    @Test
    public void testKeeperCapabilityStartFailureStopsMetaAndPreservesFailure() throws Exception {
        IllegalStateException startFailure = new IllegalStateException("keeper capability start failed");
        IllegalArgumentException stopFailure = new IllegalArgumentException("meta stop failed");
        doThrow(startFailure).when(keeperCapabilityRefreshManager).start();
        doThrow(stopFailure).when(metaChangeManager).stop();

        try {
            checker.doStart();
            Assert.fail("keeper capability start failure should propagate");
        } catch (IllegalStateException actual) {
            Assert.assertSame(startFailure, actual);
            Assert.assertArrayEquals(new Throwable[]{stopFailure}, actual.getSuppressed());
        }

        org.mockito.InOrder lifecycle = inOrder(metaChangeManager, keeperCapabilityRefreshManager);
        lifecycle.verify(metaChangeManager).start();
        lifecycle.verify(keeperCapabilityRefreshManager).start();
        lifecycle.verify(metaChangeManager).stop();
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "health-instance-load-test.xml";
    }

}
