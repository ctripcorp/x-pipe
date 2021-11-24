package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener.CrdtBacklogOffsetListener;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener.CrdtPeerBacklogOffsetListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CrdtPeerBacklogOffsetListenerTest extends AbstractCheckerTest {
    private RedisHealthCheckInstance instance;

    private CrdtPeerBacklogOffsetListener listener;

    private MetricProxy proxy;

    @Before
    public void setupBackStreamingAlertListenerTest() throws Exception {
        listener = new CrdtPeerBacklogOffsetListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, randomPort());


        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
    }

    private static final double DOUBLE_DELTA = 0.000001;
    
    @Test 
    public void testNoCrdtbacklogOffset() throws MetricProxyException {
        Mockito.doAnswer(invocation -> {
            Assert.assertTrue(false);
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());
        final String TMP_HIGH_VERSION_REPLICATION = "# CRDT Replication\r\n" +
                "ovc:1:0;2:0\r\n" +
                "gcvc:1:0;2:0\r\n" +
                "gid:1\r\n";
        InfoResultExtractor executors = new InfoResultExtractor(String.format(TMP_HIGH_VERSION_REPLICATION));
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, executors);
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);
    }

    @Test
    public void testMoreCrdtBacklogOffset() throws MetricProxyException {
        int port1 = 6379;
        int offset1 = Math.abs(randomInt());
        int port2 = 7379;
        int offset2 = 0;
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgumentAt(0, MetricData.class);
            Assert.assertEquals(CrdtPeerBacklogOffsetListener.METRIC_TYPE, point.getMetricType());
            
            if(point.getHostPort().getPort() == port1) {
                Assert.assertEquals(offset1, point.getValue(), DOUBLE_DELTA);
            } else if(point.getHostPort().getPort() == port2) {
                Assert.assertEquals(offset2, point.getValue(), DOUBLE_DELTA);   
            }
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        final String TMP_HIGH_VERSION_REPLICATION = "# CRDT Replication\r\n" +
                "ovc:1:0;2:0\r\n" +
                "gcvc:1:0;2:0\r\n" +
                "gid:1\r\n" +
                "peer0_host:127.0.0.1\r\n" + 
                "peer0_port:%d\r\n" + 
                "peer0_repl_offset:%d\r\n" +
                "peer1_host:127.0.0.1\r\n" +
                "peer1_port:%d\r\n" +
                "peer1_repl_offset:0\r\n";
        InfoResultExtractor executors = new InfoResultExtractor(String.format(TMP_HIGH_VERSION_REPLICATION, port1, offset1, port2, offset2));
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, executors);
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);


    }
}
