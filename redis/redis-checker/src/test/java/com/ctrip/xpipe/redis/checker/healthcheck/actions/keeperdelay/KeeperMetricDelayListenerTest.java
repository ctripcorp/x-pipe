package com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperInstanceInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KeeperMetricDelayListenerTest {

    private static final String CURRENT_DC = "dc-current";
    private static final String ACTIVE_DC = "dc-active";
    private static final String KEEPER_DC = "dc-keeper";
    private static final String CLUSTER = "cluster";
    private static final String SHARD = "shard";
    private static final HostPort KEEPER = new HostPort("127.0.0.1", 6379);
    private static final long SHARD_DB_ID = 42L;
    private static final long RECV_TIME_MILLI = 1_234_567_890L;

    @Mock private FoundationService foundationService;
    @Mock private MetaCache metaCache;
    @Mock private MetricProxy proxy;

    private KeeperMetricDelayListener listener;
    private DefaultKeeperHealthCheckInstance instance;
    private AtomicReference<MetricData> capturedPoint;

    @Before
    public void setUp() throws Exception {
        listener = new KeeperMetricDelayListener();
        capturedPoint = new AtomicReference<>();
        instance = instance(KEEPER_DC, ACTIVE_DC);

        ReflectionTestUtils.setField(listener, "foundationService", foundationService);
        ReflectionTestUtils.setField(listener, "metaCache", metaCache);
        ReflectionTestUtils.setField(listener, "proxy", proxy);

        when(foundationService.getDataCenter()).thenReturn(CURRENT_DC);
        when(metaCache.isCrossRegion(ACTIVE_DC, KEEPER_DC)).thenReturn(true);
        doAnswer(invocation -> {
            capturedPoint.set(invocation.getArgument(0, MetricData.class));
            return null;
        }).when(proxy).writeBinMultiDataPoint(any(MetricData.class));
    }

    @Test
    public void testKeeperPointContract() {
        listener.onAction(context(1_234_567L));

        MetricData point = capturedPoint.get();
        Assert.assertEquals("delay", point.getMetricType());
        Assert.assertEquals(KEEPER_DC, point.getDcName());
        Assert.assertEquals(CLUSTER, point.getClusterName());
        Assert.assertEquals(SHARD, point.getShardName());
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), point.getClusterType());
        Assert.assertEquals(KEEPER, point.getHostPort());
        Assert.assertEquals(1234.567, point.getValue(), 0.0);
        Assert.assertEquals(RECV_TIME_MILLI, point.getTimestampMilli());
        Assert.assertEquals(new HashSet<>(Arrays.asList("type", "isNew", "crossDc", "crossRegion")),
                point.getTags().keySet());
        Assert.assertEquals("keeper", point.getTags().get("type"));
        Assert.assertEquals("0", point.getTags().get("isNew"));
        Assert.assertEquals("true", point.getTags().get("crossDc"));
        Assert.assertEquals("true", point.getTags().get("crossRegion"));
        Assert.assertNull(point.getTags().get("state"));
        Assert.assertNull(point.getTags().get("srcShardId"));
        verify(metaCache).isCrossRegion(ACTIVE_DC, KEEPER_DC);
    }

    @Test
    public void testSameDcAndSameRegion() {
        instance = instance(CURRENT_DC, CURRENT_DC);
        when(metaCache.isCrossRegion(CURRENT_DC, CURRENT_DC)).thenReturn(false);

        listener.onAction(context(1000L));

        Assert.assertEquals("false", capturedPoint.get().getTags().get("crossDc"));
        Assert.assertEquals("false", capturedPoint.get().getTags().get("crossRegion"));
    }

    @Test
    public void testSampleLostUsesSameUnitConversion() {
        listener.onAction(context(TimeUnit.MILLISECONDS.toNanos(99_999L)));

        Assert.assertEquals(99_999_000.0, capturedPoint.get().getValue(), 0.0);
        Assert.assertEquals(RECV_TIME_MILLI, capturedPoint.get().getTimestampMilli());
    }

    @Test
    public void testMetricFailureDoesNotPropagate() throws Exception {
        doThrow(new RuntimeException("expected")).when(proxy).writeBinMultiDataPoint(any(MetricData.class));

        listener.onAction(context(1000L));
    }

    @Test
    public void testSupportsKeeperInstance() {
        Assert.assertTrue(listener.supportInstance(instance));
    }

    private DefaultKeeperHealthCheckInstance instance(String dc, String activeDc) {
        DefaultKeeperInstanceInfo info = new DefaultKeeperInstanceInfo(dc, CLUSTER, SHARD, SHARD_DB_ID,
                KEEPER, activeDc, ClusterType.ONE_WAY);
        DefaultKeeperHealthCheckInstance keeperInstance = new DefaultKeeperHealthCheckInstance();
        keeperInstance.setInstanceInfo(info);
        return keeperInstance;
    }

    private KeeperDelayActionContext context(long delayNanos) {
        KeeperDelayActionContext context = new KeeperDelayActionContext(instance, delayNanos);
        ReflectionTestUtils.setField(context, "recvTimeMilli", RECV_TIME_MILLI);
        return context;
    }
}
