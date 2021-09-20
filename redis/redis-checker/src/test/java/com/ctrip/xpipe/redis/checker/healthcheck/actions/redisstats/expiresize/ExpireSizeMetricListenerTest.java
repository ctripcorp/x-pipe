package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.expiresize;

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

public class ExpireSizeMetricListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    private ExpireSizeMetricListener listener;

    private RedisHealthCheckInstance instance;

    private ExpireSizeActionContext context;

    private long expireSize;

    private MetricProxy proxy;

    @Before
    public void setupConflictMetricListenerTest() throws Exception {
        listener = new ExpireSizeMetricListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        expireSize = Math.abs(randomInt());
        context = new ExpireSizeActionContext(instance, expireSize);

        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
    }

    @Test
    public void testOnAction() throws Exception {
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(ExpireSizeMetricListener.METRIC_TYPE, point.getMetricType());
            Assert.assertEquals(expireSize, point.getValue(), DOUBLE_DELTA);

            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Mockito.verify(proxy, Mockito.times(1)).writeBinMultiDataPoint(Mockito.any());
    }

}
