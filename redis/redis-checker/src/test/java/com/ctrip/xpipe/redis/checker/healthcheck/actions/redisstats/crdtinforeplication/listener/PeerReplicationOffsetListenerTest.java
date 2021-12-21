package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.listener;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener.PeerReplicationOffsetListener;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.CRDTInfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicReference;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PeerReplicationOffsetListenerTest extends AbstractCheckerTest {
    private RedisHealthCheckInstance instance;

    private PeerReplicationOffsetListener listener;

    private MetricProxy proxy;

    @Mock
    private MetaCache metaCache;
    
    @Before
    public void setupBackStreamingAlertListenerTest() throws Exception {
        listener = new PeerReplicationOffsetListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, randomPort());
        instance.getCheckInfo().isMaster(true);
        
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
        String info = String.format("");
        CRDTInfoResultExtractor executors = new CRDTInfoResultExtractor(info);
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, info);
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);
    }

    final String TMP_REPLICATION = "# CRDT Replication\r\n" +
            "ovc:1:0;2:0\r\n" +
            "gcvc:1:0;2:0\r\n" +
            "gid:1\r\n" +
            "peer0_host:127.0.0.1\r\n" +
            "peer0_port:%d\r\n" +
            "peer0_gid:2\r\n" + 
            "peer0_repl_offset:%d\r\n" +
            "peer1_host:127.0.0.1\r\n" +
            "peer1_port:%d\r\n" +
            "peer1_gid:3\r\n" +
            "peer1_repl_offset:0\r\n";

    final String DC_RB = "RB";
    final String DC_XY = "XY";
    
    @Test
    public void testMoreCrdtBacklogOffset() throws MetricProxyException {
        
        int port1 = 6379;
        int offset1 = Math.abs(randomInt());
        int port2 = 7379;
        int offset2 = 0;
        listener.setMetaCache(metaCache);
        HostPort host1 = new HostPort("127.0.0.1", port1);
        HostPort host2 = new HostPort("127.0.0.1", port2);
        
        Mockito.when(metaCache.getDc(host1)).thenReturn(DC_RB);
        Mockito.when(metaCache.getDc(host2)).thenReturn(DC_XY);

        String info = String.format(TMP_REPLICATION, port1, offset1, port2, offset2);
        CRDTInfoResultExtractor executors = new CRDTInfoResultExtractor(info);
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, info);
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(PeerReplicationOffsetListener.METRIC_TYPE, point.getMetricType());
            Assert.assertEquals(instance.getCheckInfo().getClusterId(), point.getClusterName());
            Assert.assertEquals(instance.getCheckInfo().getShardId(), point.getShardName());
            Assert.assertEquals(instance.getCheckInfo().getClusterType().toString(), point.getClusterType());
            Assert.assertEquals(instance.getCheckInfo().getDcId(), point.getDcName());
            Assert.assertEquals(context.getRecvTimeMilli(), point.getTimestampMilli());
            HostPort host = HostPort.fromString(point.getTags().get(PeerReplicationOffsetListener.KEY_SRC_PEER));
            Assert.assertEquals(host.getHost() , "127.0.0.1");
            if( host.getPort() == port1) {
                Assert.assertEquals(offset1, point.getValue(), DOUBLE_DELTA);
                Assert.assertEquals(point.getTags().get(PeerReplicationOffsetListener.KEY_SRC_PEER_DC), DC_RB);
            } else if(host.getPort() == port2) {
                Assert.assertEquals(offset2, point.getValue(), DOUBLE_DELTA);
                Assert.assertEquals(point.getTags().get(PeerReplicationOffsetListener.KEY_SRC_PEER_DC), DC_XY);
            }
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);
        Mockito.verify(proxy, Mockito.times(2)).writeBinMultiDataPoint(Mockito.any());
    }

    @Test
    public void testNotFindSrcPeerDc() throws MetricProxyException {
        int port1 = 6379;
        int offset1 = Math.abs(randomInt());
        int port2 = 7379;
        int offset2 = 0;
        String info = String.format(TMP_REPLICATION, port1, offset1, port2, offset2);
        HostPort host1 = new HostPort("127.0.0.1", port1);
        HostPort host2 = new HostPort("127.0.0.1", port2);
        Mockito.when(metaCache.getDc(host1)).thenReturn(DC_RB);
        Mockito.when(metaCache.getDc(host2)).thenThrow(new IllegalStateException("unfound shard for instance:" + host2));
        listener.setMetaCache(metaCache);
        
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, info);
        AtomicReference<String> host1_dc = new AtomicReference<>();
        AtomicReference<String> host2_dc = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            HostPort host = HostPort.fromString(point.getTags().get(PeerReplicationOffsetListener.KEY_SRC_PEER));
            if( host.getPort() == port1) {
                host1_dc.set(point.getTags().get((Object) PeerReplicationOffsetListener.KEY_SRC_PEER_DC));
            } else if(host.getPort() == port2) {
                host2_dc.set(point.getTags().get((Object) PeerReplicationOffsetListener.KEY_SRC_PEER_DC));
            }
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());
        listener.onAction(context);
        Mockito.verify(proxy, Mockito.times(2)).writeBinMultiDataPoint(Mockito.any());
        Assert.assertEquals(host1_dc.get(), DC_RB);
        Assert.assertEquals(host2_dc.get(), PeerReplicationOffsetListener.UNKNOWN_DC);
    }
    
}
