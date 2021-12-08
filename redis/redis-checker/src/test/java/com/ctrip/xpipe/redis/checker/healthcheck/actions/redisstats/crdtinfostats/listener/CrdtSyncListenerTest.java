package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsContext;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CrdtSyncListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    CrdtSyncListener listener;

    private RedisHealthCheckInstance instance;

    private CrdtInfoStatsContext context;
    

    private MetricProxy proxy;

    private String TEMP_OLD_STATS_RESP = "sync_full:%d\r\n" +
            "sync_backstream:%d\r\n" +
            "sync_partial_ok:%d\r\n" +
            "sync_partial_err:%d\r\n";

    @Before
    public void setupSyncListenerTest() throws Exception {
        listener = new CrdtSyncListener();
        String infoResultExtractor = String.format(TEMP_OLD_STATS_RESP, Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()));
        CRDTInfoResultExtractor extractors = new CRDTInfoResultExtractor(infoResultExtractor);
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        
        context = new CrdtInfoStatsContext(instance, infoResultExtractor);
        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
        
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgumentAt(0, MetricData.class);
            Assert.assertEquals(instance.getCheckInfo().getClusterId(), point.getClusterName());
            Assert.assertEquals(instance.getCheckInfo().getShardId(), point.getShardName());
            Assert.assertEquals(instance.getCheckInfo().getClusterType().toString(), point.getClusterType());
            Assert.assertEquals(instance.getCheckInfo().getDcId(), point.getDcName());
            Assert.assertEquals(instance.getCheckInfo().getHostPort(), point.getHostPort());
            Assert.assertEquals(context.getRecvTimeMilli(), point.getTimestampMilli());

            switch (point.getMetricType()) {
                case CrdtSyncListener.METRIC_TYPE_SYNC_FULL:
                    Assert.assertEquals(extractors.getSyncFull(), point.getValue(), DOUBLE_DELTA);
                    break;
                case CrdtSyncListener.METRIC_TYPE_SYNC_PARTIAL_OK:
                    Assert.assertEquals(extractors.getSyncPartialOk(), point.getValue(), DOUBLE_DELTA);
                    break;
                case CrdtSyncListener.METRIC_TYPE_SYNC_PARTIAL_ERR:
                    Assert.assertEquals(extractors.getSyncPartialErr(), point.getValue(), DOUBLE_DELTA);
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
        Mockito.verify(proxy, Mockito.times(3)).writeBinMultiDataPoint(Mockito.any());
    }
}
