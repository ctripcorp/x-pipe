package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
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
    private CheckerConsoleService checkerConsoleService;

    @Mock
    private CheckerConfig checkerConfig;

    @Before
    public void setupDefaultHealthCheckerMockTest() throws IOException, SAXException {
        when(checkerConfig.getIgnoredHealthCheckDc()).thenReturn(Collections.emptySet());
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
    public void generateAsymmetricHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("asymmetric_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setAzGroupName("SHA").setAzGroupType("ONE_WAY");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("asymmetric_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setAzGroupName("SHA").setAzGroupType("ONE_WAY");
        oyDcMeta.addCluster(oyClusterMeta);
        DcMeta awsDcMeta = new DcMeta("aws");
        ClusterMeta awsClusterMeta = new ClusterMeta().setId("asymmetric_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setAzGroupName("AWS").setAzGroupType("SINGLE_DC");
        SourceMeta sourceMeta = new SourceMeta().setParent(awsClusterMeta).setRegion("SHA").setSrcDc("jq").setUpstreamDc("jq");
        awsClusterMeta.addSource(sourceMeta);
        awsDcMeta.addCluster(awsClusterMeta);
        xpipeMeta.addDc(jqDcMeta).addDc(oyDcMeta).addDc(awsDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        // current dc is active dc
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(3)).getOrCreate(new ClusterMeta().setId("asymmetric_cluster"));

        //current dc is not active dc but in dr master type
        jqClusterMeta.setActiveDc("oy");
        oyClusterMeta.setActiveDc("oy");
        awsClusterMeta.setActiveDc("oy");
        sourceMeta.setSrcDc("oy").setUpstreamDc("oy");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(3)).getOrCreate(new ClusterMeta().setId("asymmetric_cluster"));

        //current dc is in master type
        jqClusterMeta.setBackupDcs("").setAzGroupName("JQ").setAzGroupType("SINGLE_DC");
        jqClusterMeta.getSources().clear();
        sourceMeta.setRegion("OY");
        oyClusterMeta.setActiveDc("oy").setBackupDcs("aws").setAzGroupName("OY");
        oyClusterMeta.addSource(sourceMeta);
        awsClusterMeta.setActiveDc("oy").setBackupDcs("aws").setAzGroupName("OY").setAzGroupType("ONE_WAY");
        oyClusterMeta.addSource(sourceMeta);
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(4)).getOrCreate(new ClusterMeta().setId("asymmetric_cluster"));

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

    @Override
    protected String getXpipeMetaConfigFile() {
        return "health-instance-load-test.xml";
    }

}
