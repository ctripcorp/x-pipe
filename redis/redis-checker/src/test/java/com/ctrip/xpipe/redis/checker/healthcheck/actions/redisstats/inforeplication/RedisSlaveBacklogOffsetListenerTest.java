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

public class RedisSlaveBacklogOffsetListenerTest extends AbstractCheckerTest {

    private static final double DOUBLE_DELTA = 0.000001;

    private RedisHealthCheckInstance instance;

    private RedisSlaveBacklogOffsetListener listener;

    private InfoReplicationContext context;

    private MetricProxy proxy;

    private String INFO_REPLICATION_RESPONSE = "$532\r\n# Replication\r\nrole:slave\r\n" +
            "master_host:127.0.0.1\r\nmaster_port:6380\r\nmaster_link_status:up\r\nmaster_last_io_seconds_ago:0\r\n" +
            "master_sync_in_progress:0\r\nslave_repl_offset:3971052969\r\nslave_priority:100\r\nslave_read_only:1\r\n" +
            "connected_slaves:0\r\nmaster_replid:eec907aaf5ad65d58bc4b06d047ad6fc88bb65d1\r\n" +
            "master_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:3971052969\r\n" +
            "second_repl_offset:-1\r\nrepl_backlog_active:1\r\nrepl_backlog_size:34603008\r\n" +
            "repl_backlog_first_byte_offset:3936449962\r\nrepl_backlog_histlen:34603008\r\n";

    @Before
    public void beforeRedisSnycListenerTest() throws Exception {
        listener = new RedisSlaveBacklogOffsetListener();
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, randomPort());

        proxy = Mockito.mock(MetricProxy.class);
        listener.setMetricProxy(proxy);
    }

    @Test
    public void testMasterBacklogOffset() throws MetricProxyException {
        testBacklogOffset(true, 0);
    }

    @Test
    public void testSlaveBacklogOffset() throws MetricProxyException {
        testBacklogOffset(false, 1);
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

            Assert.assertEquals(RedisSlaveBacklogOffsetListener.METRIC_TYPE_REDIS_SLAVE_REPL_OFFSET, point.getMetricType());
            Assert.assertEquals(extractors.getMasterReplOffset(), point.getValue(), DOUBLE_DELTA);
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);
        Mockito.verify(proxy, Mockito.times(times)).writeBinMultiDataPoint(Mockito.any());
        instance.getCheckInfo().isMaster(oldIsMasterStats);
    }
}
