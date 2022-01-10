package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.CrdtInfoStatsContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinfostats.listener.ConflictMetricListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ConflictMetricListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    ConflictMetricListener listener;
    
    private RedisHealthCheckInstance instance;

    private CrdtInfoStatsContext context;

    private ConflictMetricListener.CrdtConflictStats stats;

    private MetricProxy proxy;

    private String TEMP_STATS_RESP ="crdt_conflict:type=%d,set=%d,del=%d,set_del=%d\r\n" +
            "crdt_conflict_op:modify=%d,merge=%d\r\n";

    private String TEMP_OLD_STATS_RESP = "crdt_type_conflict:%d\r\n" +
            "crdt_non_type_conflict:%d\r\n" +
            "crdt_modify_conflict:%d\r\n" +
            "crdt_merge_conflict:%d\r\n";
    
    @Before
    public void setupConflictMetricListenerTest() throws Exception {
        listener = new ConflictMetricListener();
        String info = String.format(TEMP_OLD_STATS_RESP, Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()));
        CRDTInfoResultExtractor extractors = new CRDTInfoResultExtractor(info);
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, 6379);
        
        context = new CrdtInfoStatsContext(instance, info);
        stats = new ConflictMetricListener.CrdtConflictStats(extractors);
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
        String info = String.format(TEMP_STATS_RESP, Math.abs(randomInt()), Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()),Math.abs(randomInt()));
        CRDTInfoResultExtractor extractors = new CRDTInfoResultExtractor(info);
        context = new CrdtInfoStatsContext(instance, info);
        stats = new ConflictMetricListener.CrdtConflictStats(extractors);
        listener.onAction(context);
        Assert.assertTrue(listener.worksfor(context));
        Mockito.verify(proxy, Mockito.times(7)).writeBinMultiDataPoint(Mockito.any());
    }

}
