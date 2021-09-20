package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.conflic;

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

public class ConflictMetricListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    private ConflictMetricListener listener;

    private RedisHealthCheckInstance instance;

    private CrdtConflictCheckContext context;

    private CrdtConflictStats stats;

    private MetricProxy proxy;

    @Before
    public void setupConflictMetricListenerTest() throws Exception {
        listener = new ConflictMetricListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        stats = new CrdtConflictStats(Math.abs(randomInt()), Math.abs(randomInt()), Math.abs(randomInt()), Math.abs(randomInt()));
        context = new CrdtConflictCheckContext(instance, stats);

        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);

        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(instance.getCheckInfo().getClusterId(), point.getClusterName());
            Assert.assertEquals(instance.getCheckInfo().getShardId(), point.getShardName());
            Assert.assertEquals(instance.getCheckInfo().getClusterType().toString(), point.getClusterType());
            Assert.assertEquals(instance.getCheckInfo().getDcId(), point.getDcName());
            Assert.assertEquals(instance.getCheckInfo().getHostPort(), point.getHostPort());
            Assert.assertEquals(context.getRecvTimeMilli(), point.getTimestampMilli());

            switch (point.getMetricType()) {
                case ConflictMetricListener.METRIC_TYPE_CONFLICT:
                    Assert.assertEquals(stats.getTypeConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                case ConflictMetricListener.METRIC_NON_TYPE_CONFLICT:
                    Assert.assertEquals(stats.getNonTypeConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                case ConflictMetricListener.METRIC_MODIFY_CONFLICT:
                    Assert.assertEquals(stats.getModifyConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                case ConflictMetricListener.METRIC_MERGE_CONFLICT:
                    Assert.assertEquals(stats.getMergeConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                case ConflictMetricListener.METRIC_SET_CONFLICT:
                    Assert.assertEquals(stats.getSetConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                case ConflictMetricListener.METRIC_DEL_CONFLICT:
                    Assert.assertEquals(stats.getDelConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                case ConflictMetricListener.METRIC_SET_DEL_CONFLICT:
                    Assert.assertEquals(stats.getSetDelConflict(), point.getValue(), DOUBLE_DELTA);
                    break;
                default:
                    Assert.fail();
            }

            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());
    }

    @Test
    public void testOnActionWithOldStats() throws Exception {
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Mockito.verify(proxy, Mockito.times(4)).writeBinMultiDataPoint(Mockito.any());
    }

    @Test
    public void testOnAction() throws Exception {
        stats = new CrdtConflictStats(Math.abs(randomInt()), Math.abs(randomInt()), Math.abs(randomInt()), Math.abs(randomInt()), Math.abs(randomInt()), Math.abs(randomInt()));
        context = new CrdtConflictCheckContext(instance, stats);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Mockito.verify(proxy, Mockito.times(7)).writeBinMultiDataPoint(Mockito.any());
    }

}
