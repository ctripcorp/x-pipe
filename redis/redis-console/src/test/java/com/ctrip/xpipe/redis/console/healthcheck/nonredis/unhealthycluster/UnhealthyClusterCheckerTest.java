package com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayService;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

import static com.ctrip.xpipe.cluster.ClusterType.ONE_WAY;

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

        Mockito.when(config.getOwnClusterType()).thenReturn(Collections.singleton(ONE_WAY.toString()));
        Mockito.when(delayService.getDcActiveClusterUnhealthyInstance(Mockito.anyString())).thenReturn(new UnhealthyInfoModel());
        Mockito.when(metaCache.getClusterType(oneWayCluster)).thenReturn(ONE_WAY);
        Mockito.when(metaCache.getClusterType(biDirectionCluster)).thenReturn(ClusterType.BI_DIRECTION);
    }

    @Test
    public void testAllClusterHealthy() throws Exception {
        Mockito.doAnswer(invocation -> {
            MetricData data = invocation.getArgumentAt(0, MetricData.class);
            Assert.assertEquals(ONE_WAY.name(), data.getClusterType());
            Assert.assertEquals(0, data.getValue(), DOUBLE_DELTA);
            return null;
        }).when(metricProxy).writeBinMultiDataPoint(Mockito.any());

        checker.doCheck();
        Mockito.verify(metricProxy, Mockito.times(1)).writeBinMultiDataPoint(Mockito.any());
    }

    @Test
    public void testAllClusterUnhealthy() throws Exception {
        Mockito.when(delayService.getDcActiveClusterUnhealthyInstance(Mockito.anyString())).thenReturn(mockUnhealthyInfoModel());
        Mockito.doAnswer(invocation -> {
            MetricData data = invocation.getArgumentAt(0, MetricData.class);
            Assert.assertEquals(ONE_WAY.name(), data.getClusterType());
            Assert.assertEquals(1, data.getValue(), DOUBLE_DELTA);
            return null;
        }).when(metricProxy).writeBinMultiDataPoint(Mockito.any());

        checker.doCheck();
        Mockito.verify(metricProxy, Mockito.times(1)).writeBinMultiDataPoint(Mockito.any());
    }

    private UnhealthyInfoModel mockUnhealthyInfoModel() {
        UnhealthyInfoModel model = new UnhealthyInfoModel();
        model.addUnhealthyInstance(oneWayCluster, "dc", "shard", new HostPort());
        model.addUnhealthyInstance(biDirectionCluster, "dc", "shard", new HostPort());
        return model;
    }

}
