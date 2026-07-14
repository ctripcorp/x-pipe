package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clusterstatus;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbnormalClusterStatusMonitorTest {

    @InjectMocks
    private AbnormalClusterStatusMonitor monitor;

    @Mock
    private ClusterService clusterService;

    @Mock
    private MetricProxy metricProxy;

    @Mock
    private ConsoleCommonConfig commonConfig;

    @Before
    public void setUp() {
        monitor.setMetricProxy(metricProxy);
        when(commonConfig.getAbnormalClusterStatusMonitorIntervalMilli()).thenReturn(60_000L);
    }

    @Test
    public void testReportAbnormalClusters() throws MetricProxyException {
        ClusterTbl cluster = new ClusterTbl();
        cluster.setClusterName("cluster1");
        cluster.setStatus("Migrating");
        cluster.setClusterType("one_way");

        MigrationClusterTbl migrationCluster = new MigrationClusterTbl();
        migrationCluster.setStatus("Migrating");
        cluster.setMigrationClusters(migrationCluster);

        when(clusterService.findMigratingClusters()).thenReturn(Collections.singletonList(cluster));

        monitor.doAction();

        ArgumentCaptor<MetricData> metricCaptor = ArgumentCaptor.forClass(MetricData.class);
        verify(metricProxy, times(1)).writeBinMultiDataPoint(metricCaptor.capture());

        MetricData metricData = metricCaptor.getValue();
        Assert.assertEquals(AbnormalClusterStatusMonitor.METRIC_TYPE, metricData.getMetricType());
        Assert.assertEquals("cluster1", metricData.getClusterName());
        Assert.assertEquals(1D, metricData.getValue(), 0);
        Assert.assertEquals("cluster1", metricData.getTags().get("clusterName"));
        Assert.assertEquals("Migrating", metricData.getTags().get("clusterStatus"));
        Assert.assertEquals("Migrating", metricData.getTags().get("migrationStatus"));
        Assert.assertEquals("one_way", metricData.getTags().get("clusterType"));
        Assert.assertNull(metricData.getTags().get("sourceDc"));
        Assert.assertNull(metricData.getTags().get("destDc"));
        Assert.assertNull(metricData.getTags().get("activeDc"));
    }

    @Test
    public void testReportWithoutMigrationOverview() throws MetricProxyException {
        ClusterTbl cluster = new ClusterTbl();
        cluster.setClusterName("cluster2");
        cluster.setStatus("Lock");
        cluster.setClusterType("bi_direction");

        when(clusterService.findMigratingClusters()).thenReturn(Collections.singletonList(cluster));

        monitor.doAction();

        ArgumentCaptor<MetricData> metricCaptor = ArgumentCaptor.forClass(MetricData.class);
        verify(metricProxy, times(1)).writeBinMultiDataPoint(metricCaptor.capture());

        MetricData metricData = metricCaptor.getValue();
        Assert.assertEquals("Lock", metricData.getTags().get("clusterStatus"));
        Assert.assertEquals("", metricData.getTags().get("migrationStatus"));
        Assert.assertEquals("bi_direction", metricData.getTags().get("clusterType"));
    }

    @Test
    public void testInterval() {
        Assert.assertEquals(60_000L, monitor.getIntervalMilli());
        Assert.assertEquals(60_000L, monitor.getLeastIntervalMilli());
    }
}
