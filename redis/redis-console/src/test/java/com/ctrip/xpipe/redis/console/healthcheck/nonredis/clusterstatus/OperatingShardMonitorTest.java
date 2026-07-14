package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clusterstatus;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OperatingShardMonitorTest {

    @InjectMocks
    private OperatingShardMonitor monitor;

    @Mock
    private MetaCache metaCache;

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
    public void testReportOperatingShards() throws MetricProxyException {
        ShardMeta shard1 = new ShardMeta("shard1");
        shard1.setOperatingUntil(System.currentTimeMillis() + 60_000L);
        ShardMeta shard2 = new ShardMeta("shard2");
        shard2.setOperatingUntil(System.currentTimeMillis() + 60_000L);
        ClusterMeta clusterMeta = new ClusterMeta("cluster1");
        clusterMeta.addShard(shard1);
        clusterMeta.addShard(shard2);
        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.addCluster(clusterMeta);
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

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
        ShardMeta shard1 = new ShardMeta("shard1");
        ClusterMeta clusterMeta = new ClusterMeta("cluster1");
        clusterMeta.addShard(shard1);
        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.addCluster(clusterMeta);
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dcMeta);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        monitor.doAction();

        verifyNoInteractions(metricProxy);
    }

    @Test
    public void testSkipWhenMetaNotReady() throws MetricProxyException {
        when(metaCache.getXpipeMeta()).thenReturn(null);

        monitor.doAction();

        verifyNoInteractions(metricProxy);
    }

    @Test
    public void testInterval() {
        Assert.assertEquals(60_000L, monitor.getIntervalMilli());
        Assert.assertEquals(60_000L, monitor.getLeastIntervalMilli());
    }
}
