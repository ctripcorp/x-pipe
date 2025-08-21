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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PeerBacklogOffsetListenerTest extends AbstractCheckerTest {
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

    public void testCrdtBacklogOffset(boolean isMaster, int times) throws MetricProxyException {
        int offset = Math.abs(randomInt());
        boolean oldIsMasterStats = instance.getCheckInfo().isMaster();
        instance.getCheckInfo().isMaster(isMaster);
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(PeerBacklogOffsetListener.METRIC_TYPE, point.getMetricType());
            Assert.assertEquals(offset, point.getValue(), DOUBLE_DELTA);
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, String.format(TMP_REPLICATION, offset));
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);
        Mockito.verify(proxy, Mockito.times(times)).writeBinMultiDataPoint(Mockito.any());
        instance.getCheckInfo().isMaster(oldIsMasterStats);
    }
    
    @Test
    public void testMasterCrdtBacklogOffset() throws MetricProxyException {
        testCrdtBacklogOffset(true, 1);
        
    }

    @Test
    public void testSlaverCrdtBacklogOffset() throws  MetricProxyException {
        testCrdtBacklogOffset(false, 0);
    }
    
}
    
    
