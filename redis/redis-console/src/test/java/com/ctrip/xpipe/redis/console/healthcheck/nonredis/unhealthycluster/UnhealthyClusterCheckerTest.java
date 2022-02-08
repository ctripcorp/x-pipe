package com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.cluster.ClusterType.BI_DIRECTION;
import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;
import static com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster.UnhealthyClusterChecker.UNHEALTHY_CLUSTER_METRIC_TYPE;
import static com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster.UnhealthyClusterChecker.UNHEALTHY_INSTANCE_METRIC_TYPE;

/**
 * @Author lishanglin
 * @Date 2020/11/10
 */
@RunWith(MockitoJUnitRunner.class)
public class UnhealthyClusterCheckerTest extends AbstractConsoleTest {

    private UnhealthyClusterChecker checker;

    @Mock
    private DelayService delayService;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleConfig config;

    @Mock
    private MetricProxy metricProxy;

    private String oneWayCluster = "cluster1";

    private String biDirectionCluster = "cluster2";

    private static final double DOUBLE_DELTA = 0.000001;

    @Before
    public void setupUnhealthyClusterCheckerTest() {
        checker = new UnhealthyClusterChecker(delayService, metaCache, config);
        checker.setMetricProxy(metricProxy);

        Mockito.when(config.getOwnClusterType()).thenReturn(Sets.newHashSet(ONE_WAY.name(), BI_DIRECTION.name()));
        Mockito.when(delayService.getDcActiveClusterUnhealthyInstance(Mockito.anyString())).thenReturn(new UnhealthyInfoModel());
        Mockito.when(metaCache.getClusterType(oneWayCluster)).thenReturn(ONE_WAY);
        Mockito.when(metaCache.getClusterType(biDirectionCluster)).thenReturn(ClusterType.BI_DIRECTION);
    }

    @Test
    public void testAllClusterHealthy() throws Exception {
        AtomicInteger unhealthyClusters = new AtomicInteger(0);
        AtomicInteger unhealthyInstances = new AtomicInteger(0);
        Mockito.doAnswer(invocation -> {
            MetricData data = invocation.getArgument(0, MetricData.class);
            if (data.getMetricType().equals(UNHEALTHY_CLUSTER_METRIC_TYPE)) {
                unhealthyClusters.addAndGet((int)data.getValue());
            } else if (data.getMetricType().equals(UNHEALTHY_INSTANCE_METRIC_TYPE)) {
                unhealthyInstances.addAndGet((int)data.getValue());
            } else {
                Assert.fail("unexpected metric type " + data.getMetricType());
            }
            return null;
        }).when(metricProxy).writeBinMultiDataPoint(Mockito.any());

        checker.doCheck();
        Mockito.verify(metricProxy, Mockito.times(2)).writeBinMultiDataPoint(Mockito.any());
        Assert.assertEquals(0, unhealthyClusters.get());
        Assert.assertEquals(0, unhealthyInstances.get());
    }

    @Test
    public void testAllClusterUnhealthy() throws Exception {
        AtomicInteger unhealthyClusters = new AtomicInteger(0);
        AtomicInteger unhealthyInstances = new AtomicInteger(0);

        Mockito.when(delayService.getDcActiveClusterUnhealthyInstance(Mockito.anyString())).thenReturn(mockUnhealthyInfoModel());
        Mockito.doAnswer(invocation -> {
            MetricData data = invocation.getArgument(0, MetricData.class);
            if (data.getMetricType().equals(UNHEALTHY_CLUSTER_METRIC_TYPE)) {
                unhealthyClusters.addAndGet((int)data.getValue());
            } else if (data.getMetricType().equals(UNHEALTHY_INSTANCE_METRIC_TYPE)) {
                unhealthyInstances.addAndGet((int)data.getValue());
            } else {
                Assert.fail("unexpected metric type " + data.getMetricType());
            }
            return null;
        }).when(metricProxy).writeBinMultiDataPoint(Mockito.any());

        checker.doCheck();
        Mockito.verify(metricProxy, Mockito.times(4)).writeBinMultiDataPoint(Mockito.any());
        Assert.assertEquals(2, unhealthyClusters.get());
        Assert.assertEquals(2, unhealthyInstances.get());
    }

    private UnhealthyInfoModel mockUnhealthyInfoModel() {
        UnhealthyInfoModel model = new UnhealthyInfoModel();
        model.addUnhealthyInstance(oneWayCluster, "dc", "shard", new HostPort(), true);
        model.addUnhealthyInstance(biDirectionCluster, "dc", "shard", new HostPort(), false);
        return model;
    }

}
