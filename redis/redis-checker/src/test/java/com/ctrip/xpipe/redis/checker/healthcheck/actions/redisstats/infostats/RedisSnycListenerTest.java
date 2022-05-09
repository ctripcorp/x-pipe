package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.infostats;


import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RedisSnycListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    private RedisHealthCheckInstance instance;

    private RedisSnycListener listener;

    private InfoStatsContext context;

    private MetricProxy proxy;

    private static final String INFO_STATS_RESPONSE = "sync_full:99\r\n" +
            "sync_partial_ok:24\r\n" +
            "sync_partial_err:97\r\n";

    @Before
    public void beforeRedisSnycListenerTest() throws Exception {
        listener = new RedisSnycListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, randomPort());

        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);

        InfoResultExtractor extractors = new InfoResultExtractor(INFO_STATS_RESPONSE);
        context = new InfoStatsContext(instance, INFO_STATS_RESPONSE);

        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(instance.getCheckInfo().getClusterId(), point.getClusterName());
            Assert.assertEquals(instance.getCheckInfo().getShardId(), point.getShardName());
            Assert.assertEquals(instance.getCheckInfo().getClusterType().toString(), point.getClusterType());
            Assert.assertEquals(instance.getCheckInfo().getDcId(), point.getDcName());
            Assert.assertEquals(instance.getCheckInfo().getHostPort(), point.getHostPort());
            Assert.assertEquals(context.getRecvTimeMilli(), point.getTimestampMilli());

            switch (point.getMetricType()) {
                case RedisSnycListener.METRIC_TYPE_REDIS_SYNC_FULL:
                    Assert.assertEquals(extractors.getSyncFull(), point.getValue(), DOUBLE_DELTA);
                    break;
                case RedisSnycListener.METRIC_TYPE_REDIS_SYNC_PARTIAL_OK:
                    Assert.assertEquals(extractors.getSyncPartialOk(), point.getValue(), DOUBLE_DELTA);
                    break;
                case RedisSnycListener.METRIC_TYPE_REDIS_SYNC_PARTIAL_ERR:
                    Assert.assertEquals(extractors.getSyncPartialErr(), point.getValue(), DOUBLE_DELTA);
                    break;
                default:
                    Assert.fail();
            }
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());
    }

    @Test
    public void testRedisSync() throws MetricProxyException {
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Mockito.verify(proxy, Mockito.times(3)).writeBinMultiDataPoint(Mockito.any());
    }
}
