package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.MetaChangeManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
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
    private CheckerConfig checkerConfig;

    @Before
    public void setupDefaultHealthCheckerMockTest() {
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
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("cross_dc_cluster").setType("CROSS_DC").setActiveDc("oy").setDcs("jq,oy").setDcGroupName("jq");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("cross_dc_cluster").setType("CROSS_DC").setActiveDc("oy").setDcs("jq,oy").setDcGroupName("oy");
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
        ClusterMeta clusterMeta = new ClusterMeta().setId("single_dc_cluster").setType("SINGLE_DC").setActiveDc("jq").setDcGroupName("jq");
        jqDcMeta.addCluster(clusterMeta);
        xpipeMeta.addDc(jqDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        checker.generateHealthCheckInstances();
        verify(instanceManager, times(1)).getOrCreate(new ClusterMeta().setId("single_dc_cluster"));

        clusterMeta.setActiveDc("oy").setDcGroupName("oy");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(1)).getOrCreate(new ClusterMeta().setId("single_dc_cluster"));
    }

    @Test
    public void generateLocalDCHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("local_dc_cluster").setType("LOCAL_DC").setDcs("jq,oy").setDcGroupName("jq");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("local_dc_cluster").setType("LOCAL_DC").setDcs("jq,oy").setDcGroupName("oy");
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
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setDcGroupName("jq").setDcGroupType("DR_MASTER");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("one_way_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setDcGroupName("oy").setDcGroupType("DR_MASTER");
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
    public void generateHeteroHealthCheckInstancesTest() throws Exception {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jqDcMeta = new DcMeta("jq");
        ClusterMeta jqClusterMeta = new ClusterMeta().setId("hetero_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setDcGroupName("jq").setDcGroupType("DR_MASTER");
        jqDcMeta.addCluster(jqClusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        ClusterMeta oyClusterMeta = new ClusterMeta().setId("hetero_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setDcGroupName("oy").setDcGroupType("DR_MASTER");
        oyDcMeta.addCluster(oyClusterMeta);
        DcMeta awsDcMeta = new DcMeta("aws");
        ClusterMeta awsClusterMeta = new ClusterMeta().setId("hetero_cluster").setType("one_way").setActiveDc("jq").setBackupDcs("oy").setDcGroupName("aws").setDcGroupType("MASTER");
        awsDcMeta.addCluster(awsClusterMeta);
        xpipeMeta.addDc(jqDcMeta).addDc(oyDcMeta).addDc(awsDcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        // current dc is active dc
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(3)).getOrCreate(new ClusterMeta().setId("hetero_cluster"));

        //current dc is not active dc but in dr master type
        jqClusterMeta.setActiveDc("oy");
        oyClusterMeta.setActiveDc("oy");
        awsClusterMeta.setActiveDc("oy");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(3)).getOrCreate(new ClusterMeta().setId("hetero_cluster"));

        //current dc is in master type
        jqClusterMeta.setDcGroupType("MASTER");
        oyClusterMeta.setDcGroupType("DR_MASTER");
        awsClusterMeta.setDcGroupType("DR_MASTER");
        checker.generateHealthCheckInstances();
        verify(instanceManager, times(4)).getOrCreate(new ClusterMeta().setId("hetero_cluster"));

    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "health-instance-load-test.xml";
    }

}
