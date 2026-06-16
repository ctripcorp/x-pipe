package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

public class MetricDelayListenerTest extends AbstractCheckerTest {

    private static final int WINDOW_MINUTES = 15;

    private static final long MILLIS_PER_MINUTE = 60_000L;

    private MetricDelayListener listener;

    private MetricProxy proxy;

    private CommonConfigBean commonConfigBean;

    private RedisHealthCheckInstance instance;

    private AtomicReference<MetricData> capturedPoint;

    @Before
    public void setupMetricDelayListenerTest() throws Exception {
        listener = new MetricDelayListener();
        proxy = Mockito.mock(MetricProxy.class);
        commonConfigBean = Mockito.mock(CommonConfigBean.class);
        capturedPoint = new AtomicReference<>();

        ReflectionTestUtils.setField(listener, "proxy", proxy);
        ReflectionTestUtils.setField(listener, "commonConfigBean", commonConfigBean);
        ReflectionTestUtils.setField(listener, "foundationService", FoundationService.DEFAULT);

        Mockito.when(commonConfigBean.getDelayMetricNewInstanceMinutes()).thenReturn(WINDOW_MINUTES);

        Mockito.doAnswer(invocation -> {
            capturedPoint.set(invocation.getArgument(0, MetricData.class));
            return null;
        }).when(proxy).writeBinMultiDataPoint(Mockito.any());

        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.ONE_WAY, randomPort());
    }

    @Test
    public void testIsNewWithinWindow() throws Exception {
        setCreateTime(new Date());

        listener.onAction(new DelayActionContext(instance, 1000L));

        Assert.assertEquals("1", capturedPoint.get().getTags().get("isNew"));
        Assert.assertNull(capturedPoint.get().getTags().get("delayType"));
    }

    @Test
    public void testIsNewOutsideWindow() throws Exception {
        long recvTime = System.currentTimeMillis();
        setCreateTime(new Date(recvTime - 20 * MILLIS_PER_MINUTE));

        listener.onAction(contextWithRecvTime(recvTime));

        Assert.assertEquals("0", capturedPoint.get().getTags().get("isNew"));
        Assert.assertNull(capturedPoint.get().getTags().get("delayType"));
    }

    @Test
    public void testIsNewWhenCreateTimeNull() throws Exception {
        setCreateTime(null);

        listener.onAction(new DelayActionContext(instance, 1000L));

        Assert.assertEquals("0", capturedPoint.get().getTags().get("isNew"));
        Assert.assertNull(capturedPoint.get().getTags().get("delayType"));
    }

    @Test
    public void testIsNewEpochZero() throws Exception {
        long recvTime = System.currentTimeMillis();
        setCreateTime(new Date(0L));

        listener.onAction(contextWithRecvTime(recvTime));

        Assert.assertEquals("0", capturedPoint.get().getTags().get("isNew"));
    }

    @Test
    public void testIsNewAtWindowBoundary() throws Exception {
        long recvTime = System.currentTimeMillis();
        setCreateTime(new Date(recvTime - WINDOW_MINUTES * MILLIS_PER_MINUTE));

        listener.onAction(contextWithRecvTime(recvTime));

        Assert.assertEquals("0", capturedPoint.get().getTags().get("isNew"));
    }

    @Test
    public void testIsNewJustInsideWindow() throws Exception {
        long recvTime = System.currentTimeMillis();
        setCreateTime(new Date(recvTime - WINDOW_MINUTES * MILLIS_PER_MINUTE + 1));

        listener.onAction(contextWithRecvTime(recvTime));

        Assert.assertEquals("1", capturedPoint.get().getTags().get("isNew"));
    }

    @Test
    public void testIsNewFutureCreateTime() throws Exception {
        long recvTime = System.currentTimeMillis();
        setCreateTime(new Date(recvTime + MILLIS_PER_MINUTE));

        listener.onAction(contextWithRecvTime(recvTime));

        Assert.assertEquals("1", capturedPoint.get().getTags().get("isNew"));
    }

    @Test
    public void testIsNewInstanceUnit() {
        long recvTime = 1_000_000L;
        Date createTime = new Date(recvTime - WINDOW_MINUTES * MILLIS_PER_MINUTE);

        Assert.assertFalse(MetricDelayListener.isNewInstance(createTime, recvTime, WINDOW_MINUTES));
        Assert.assertTrue(MetricDelayListener.isNewInstance(createTime, recvTime - 1, WINDOW_MINUTES));
        Assert.assertFalse(MetricDelayListener.isNewInstance(null, recvTime, WINDOW_MINUTES));
    }

    private DelayActionContext contextWithRecvTime(long recvTimeMilli) {
        DelayActionContext context = new DelayActionContext(instance, 1000L);
        ReflectionTestUtils.setField(context, "recvTimeMilli", recvTimeMilli);
        return context;
    }

    private void setCreateTime(Date createTime) {
        DefaultRedisInstanceInfo info = (DefaultRedisInstanceInfo) instance.getCheckInfo();
        info.setCreateTime(createTime);
    }
}
