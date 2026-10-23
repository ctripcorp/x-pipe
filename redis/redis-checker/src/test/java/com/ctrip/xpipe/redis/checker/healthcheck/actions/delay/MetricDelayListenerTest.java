package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.keeperstate.KeeperStoreTypeResolver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

public class MetricDelayListenerTest extends AbstractCheckerTest {

    private static final int WINDOW_MINUTES = 15;

    private static final long MILLIS_PER_MINUTE = 60_000L;

    private MetricDelayListener listener;

    private MetricProxy proxy;

    private CommonConfigBean commonConfigBean;

    private KeeperStoreTypeResolver keeperStoreTypeResolver;

    private RedisHealthCheckInstance instance;

    private AtomicReference<MetricData> capturedPoint;

    @Before
    public void setupMetricDelayListenerTest() throws Exception {
        listener = new MetricDelayListener();
        proxy = Mockito.mock(MetricProxy.class);
        commonConfigBean = Mockito.mock(CommonConfigBean.class);
        capturedPoint = new AtomicReference<>();

        keeperStoreTypeResolver = Mockito.mock(KeeperStoreTypeResolver.class);

        ReflectionTestUtils.setField(listener, "proxy", proxy);
        ReflectionTestUtils.setField(listener, "commonConfigBean", commonConfigBean);
        ReflectionTestUtils.setField(listener, "foundationService", FoundationService.DEFAULT);
        ReflectionTestUtils.setField(listener, "keeperStoreTypeResolver", keeperStoreTypeResolver);

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
    public void testRedisPointContract() {
        long recvTime = 1_234_567_890L;
        DefaultRedisInstanceInfo info = (DefaultRedisInstanceInfo) instance.getCheckInfo();
        info.setCreateTime(new Date(recvTime - 20 * MILLIS_PER_MINUTE));
        info.setCrossRegion(false);
        DelayActionContext context = new DelayActionContext(instance, 1_234_567L);
        ReflectionTestUtils.setField(context, "recvTimeMilli", recvTime);

        listener.onAction(context);

        MetricData point = capturedPoint.get();
        Assert.assertEquals("delay", point.getMetricType());
        Assert.assertEquals(info.getDcId(), point.getDcName());
        Assert.assertEquals(info.getClusterId(), point.getClusterName());
        Assert.assertEquals(info.getShardId(), point.getShardName());
        Assert.assertEquals(info.getClusterType().toString(), point.getClusterType());
        Assert.assertEquals(info.getHostPort(), point.getHostPort());
        Assert.assertEquals(1234.567, point.getValue(), 0.0);
        Assert.assertEquals(recvTime, point.getTimestampMilli());
        Assert.assertEquals(new HashSet<>(Arrays.asList("type", "isNew", "crossDc", "crossRegion", "role")),
                point.getTags().keySet());
        Assert.assertEquals("redis", point.getTags().get("type"));
        Assert.assertEquals("0", point.getTags().get("isNew"));
        Assert.assertEquals("false", point.getTags().get("crossDc"));
        Assert.assertEquals("false", point.getTags().get("crossRegion"));
        Assert.assertNull(point.getTags().get("srcShardId"));
    }

    /** redis 侧 role 取自 meta 的 isMaster，取值域与 keeper 的 active/backup 不同。 */
    @Test
    public void testRoleTagFollowsMasterFlag() {
        DefaultRedisInstanceInfo info = (DefaultRedisInstanceInfo) instance.getCheckInfo();

        info.isMaster(true);
        listener.onAction(new DelayActionContext(instance, 1000L));
        Assert.assertEquals("master", capturedPoint.get().getTags().get("role"));

        info.isMaster(false);
        listener.onAction(new DelayActionContext(instance, 1000L));
        Assert.assertEquals("slave", capturedPoint.get().getTags().get("role"));
    }

    @Test
    public void testKeeperStoreTypeTagFromResolver() {
        Mockito.when(keeperStoreTypeResolver.resolve(Mockito.any())).thenReturn("10");

        listener.onAction(new DelayActionContext(instance, 1000L));

        Assert.assertEquals("10", capturedPoint.get().getTags().get("keeperStoreType"));
    }

    /** 解析不出组合时只是少一个 tag，延迟本身照常上报。 */
    @Test
    public void testKeeperStoreTypeTagOmittedWhenUnresolved() {
        Mockito.when(keeperStoreTypeResolver.resolve(Mockito.any())).thenReturn(null);

        listener.onAction(new DelayActionContext(instance, 1000L));

        Assert.assertNotNull(capturedPoint.get());
        Assert.assertNull(capturedPoint.get().getTags().get("keeperStoreType"));
    }

    /** onAction 会吞异常，解析失败若放任抛出就会连 delay 主指标一起丢掉。 */
    @Test
    public void testResolverFailureStillReportsDelay() {
        Mockito.when(keeperStoreTypeResolver.resolve(Mockito.any()))
                .thenThrow(new RuntimeException("expected"));

        listener.onAction(new DelayActionContext(instance, 1000L));

        Assert.assertNotNull("delay 必须照常上报", capturedPoint.get());
        Assert.assertNull(capturedPoint.get().getTags().get("keeperStoreType"));
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
