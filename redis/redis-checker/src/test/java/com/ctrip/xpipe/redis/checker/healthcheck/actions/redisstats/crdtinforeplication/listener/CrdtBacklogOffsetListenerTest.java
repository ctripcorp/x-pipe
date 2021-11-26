package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.listener;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener.PeerBacklogOffsetListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CrdtBacklogOffsetListenerTest extends AbstractCheckerTest {
    private RedisHealthCheckInstance instance;

    private PeerBacklogOffsetListener listener;

    private MetricProxy proxy;

    @Before
    public void setupBackStreamingAlertListenerTest() throws Exception {
        listener = new PeerBacklogOffsetListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, randomPort());


        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
    }

    private static final double DOUBLE_DELTA = 0.000001;

    final String TMP_REPLICATION = "# CRDT Replication\r\n" +
            "ovc:1:0;2:0\r\n" +
            "gcvc:1:0;2:0\r\n" +
            "gid:1\r\n" +
            "master_repl_offset:%d\r\n";

    @Test
    public void testCrdtBacklogOffset() throws MetricProxyException {
        int offset = Math.abs(randomInt());
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgumentAt(0, MetricData.class);
            Assert.assertEquals(PeerBacklogOffsetListener.METRIC_TYPE, point.getMetricType());
            Assert.assertEquals(offset, point.getValue(), DOUBLE_DELTA);

            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        
        InfoResultExtractor executors = new InfoResultExtractor(String.format(TMP_REPLICATION, offset));
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, executors);
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);


    }
    
}
    
    
