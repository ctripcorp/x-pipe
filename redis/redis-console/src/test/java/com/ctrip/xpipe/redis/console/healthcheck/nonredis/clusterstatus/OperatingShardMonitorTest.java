package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clusterstatus;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OperatingShardMonitorTest {

    @InjectMocks
    private OperatingShardMonitor monitor;

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Mock
    private MetricProxy metricProxy;

    @Mock
    private ConsoleConfig consoleConfig;

    @Before
    public void setUp() {
        monitor.setMetricProxy(metricProxy);
        when(consoleConfig.getAbnormalClusterStatusMonitorIntervalMilli()).thenReturn(30_000L);
    }

    @Test
    public void testReportOperatingShards() throws MetricProxyException {
        DcClusterShardTbl shard1 = mock(DcClusterShardTbl.class);
        when(shard1.getDcName()).thenReturn("jq");
        when(shard1.getClusterName()).thenReturn("cluster1");
        when(shard1.getShardName()).thenReturn("shard1");
        DcClusterShardTbl shard2 = mock(DcClusterShardTbl.class);
        when(shard2.getDcName()).thenReturn("jq");
        when(shard2.getClusterName()).thenReturn("cluster1");
        when(shard2.getShardName()).thenReturn("shard2");
        when(dcClusterShardService.findOperatingDcClusterShards()).thenReturn(Arrays.asList(shard1, shard2));

        monitor.doAction();

        ArgumentCaptor<MetricData> metricCaptor = ArgumentCaptor.forClass(MetricData.class);
        verify(metricProxy, times(2)).writeBinMultiDataPoint(metricCaptor.capture());

        MetricData metricData = metricCaptor.getAllValues().get(0);
        Assert.assertEquals(OperatingShardMonitor.METRIC_TYPE, metricData.getMetricType());
        Assert.assertEquals("jq", metricData.getTags().get("dcName"));
        Assert.assertEquals("cluster1", metricData.getTags().get("clusterName"));
        Assert.assertEquals("shard1", metricData.getTags().get("shardName"));
        Assert.assertNull(metricData.getTags().get("clusterType"));
    }

    @Test
    public void testSkipWhenNoOperatingShard() throws MetricProxyException {
        when(dcClusterShardService.findOperatingDcClusterShards()).thenReturn(Collections.emptyList());

        monitor.doAction();

        verifyNoInteractions(metricProxy);
    }

    @Test
    public void testInterval() {
        Assert.assertEquals(30_000L, monitor.getIntervalMilli());
        Assert.assertEquals(30_000L, monitor.getLeastIntervalMilli());
    }
}
