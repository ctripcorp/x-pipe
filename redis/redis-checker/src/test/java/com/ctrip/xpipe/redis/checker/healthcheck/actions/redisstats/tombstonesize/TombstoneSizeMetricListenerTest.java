package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.tombstonesize;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TombstoneSizeMetricListenerTest extends AbstractCheckerTest {
    private static final double DOUBLE_DELTA = 0.000001;

    private TombstoneSizeMetricListener listener;

    private RedisHealthCheckInstance instance;

    private TombstoneSizeActionContext context;

    private long tombstoneSize;

    private MetricProxy proxy;

    @Before
    public void setupConflictMetricListenerTest() throws Exception {
        listener = new TombstoneSizeMetricListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        tombstoneSize = Math.abs(randomInt());
        context = new TombstoneSizeActionContext(instance, tombstoneSize);

        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
    }

    @Test
    public void testOnAction() throws Exception {
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(TombstoneSizeMetricListener.METRIC_TYPE, point.getMetricType());
            Assert.assertEquals(tombstoneSize, point.getValue(), DOUBLE_DELTA);

            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Mockito.verify(proxy, Mockito.times(1)).writeBinMultiDataPoint(Mockito.any());
    }

}
