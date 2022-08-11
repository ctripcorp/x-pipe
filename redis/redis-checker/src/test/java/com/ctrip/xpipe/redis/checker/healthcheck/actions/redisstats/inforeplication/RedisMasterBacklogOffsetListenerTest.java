package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.inforeplication;

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

public class RedisMasterBacklogOffsetListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    private RedisHealthCheckInstance instance;

    private RedisMasterBacklogOffsetListener listener;

    private InfoReplicationContext context;

    private MetricProxy proxy;

    private String INFO_REPLICATION_RESPONSE = "role:master\r\nconnected_slaves:2\r\n" +
            "slave0:ip=10.2.37.210,port=7788,state=online,offset=11737254120,lag=0\r\n" +
            "slave1:ip=10.5.69.195,port=6380,state=online,offset=11737253859,lag=0\r\n" +
            "master_replid:d6d60a0206867600224b0002df39b364d1e7dc1c\r\n" +
            "master_replid2:0e7f6ec47d6f83731410e313a6d570c12cecef86\r\n" +
            "master_repl_offset:11737254319\r\nsecond_repl_offset:6519820847\r\nrepl_backlog_active:1\r\n" +
            "repl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:11736205744\r\nrepl_backlog_histlen:1048576\r\n";
    @Before
    public void beforeRedisSnycListenerTest() throws Exception {
        listener = new RedisMasterBacklogOffsetListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, randomPort());

        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
    }

    @Test
    public void testMasterBacklogOffset() throws MetricProxyException {
        testBacklogOffset(true, 1);
    }

    @Test
    public void testSlaveBacklogOffset() throws MetricProxyException {
        testBacklogOffset(false, 0);
    }

    private void testBacklogOffset(boolean isMaster, int times) throws MetricProxyException {
        boolean oldIsMasterStats = instance.getCheckInfo().isMaster();
        instance.getCheckInfo().isMaster(isMaster);

        InfoResultExtractor extractors = new InfoResultExtractor(INFO_REPLICATION_RESPONSE);
        context = new InfoReplicationContext(instance, INFO_REPLICATION_RESPONSE);

        Mockito.doAnswer(invocation -> {
            MetricData point = invocation.getArgument(0, MetricData.class);
            Assert.assertEquals(instance.getCheckInfo().getClusterId(), point.getClusterName());
            Assert.assertEquals(instance.getCheckInfo().getShardId(), point.getShardName());
            Assert.assertEquals(instance.getCheckInfo().getClusterType().toString(), point.getClusterType());
            Assert.assertEquals(instance.getCheckInfo().getDcId(), point.getDcName());
            Assert.assertEquals(instance.getCheckInfo().getHostPort(), point.getHostPort());
            Assert.assertEquals(context.getRecvTimeMilli(), point.getTimestampMilli());

            Assert.assertEquals(RedisMasterBacklogOffsetListener.METRIC_TYPE_REDIS_MASTER_REPL_OFFSET, point.getMetricType());
            Assert.assertEquals(extractors.getMasterReplOffset(), point.getValue(), DOUBLE_DELTA);
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);
        Mockito.verify(proxy, Mockito.times(times)).writeBinMultiDataPoint(Mockito.any());
        instance.getCheckInfo().isMaster(oldIsMasterStats);
    }
}
