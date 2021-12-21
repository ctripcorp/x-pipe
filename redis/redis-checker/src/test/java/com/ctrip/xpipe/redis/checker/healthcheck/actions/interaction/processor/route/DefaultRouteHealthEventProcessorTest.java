package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.ProxyManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceHalfSick;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.HealthEventProcessorException;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.*;

import static org.mockito.Mockito.*;

public class DefaultRouteHealthEventProcessorTest extends AbstractTest {

    private DefaultRouteHealthEventProcessor processor = new DefaultRouteHealthEventProcessor() {
        @Override
        protected long getHoldingMillis() {
            return 100;
        }
    };

    @Mock
    private MetaCache metaCache;

    @Mock
    private RedisSessionManager redisSessionManager;

    @Mock
    private ProxyManager proxyManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private RedisSession redisSession;

    @Mock
    private InfoResultExtractor infoResultExtractor;

    private ProxyTunnelInfo proxyTunnelInfo;

    @Before
    public void beforeRouteHealthEventProcessorTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        processor.metaCache = metaCache;
        processor.proxyManager = proxyManager;
        processor.redisSessionManager = redisSessionManager;
        processor.scheduled = Executors.newScheduledThreadPool(1);
        processor = spy(processor);

        proxyTunnelInfo = new ProxyTunnelInfo();

        when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo("FRA-AWS", "cluster", "shard", new HostPort("127.0.0.3", 6379), "SHAJQ", ClusterType.ONE_WAY));
        when(instance.getRedisSession()).thenReturn(redisSession);
        when(redisSessionManager.findOrCreateSession(any(HostPort.class))).thenReturn(redisSession);
        when(metaCache.findMaster("cluster", "shard")).thenReturn(new HostPort("127.0.0.3", 6379));

    }

    @Test
    public void testOnEventWithSickWithNoProxyChain() throws HealthEventProcessorException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(null);
        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, never()).tryRecover(any(), any());
    }

    @Test
    public void testOnEventWithSickWithPartialSync() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(proxyTunnelInfo);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(0);

        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, times(1)).tryRecover(any(), any());
    }

    @Test
    public void testOnEventWithSickWithFullSyncBlockNow() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(proxyTunnelInfo);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(1);
        when(infoResultExtractor.extractAsLong("rdb_last_cow_size")).thenReturn(1024L);

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.delayDownAfterMilli()).thenReturn((int) TimeUnit.MINUTES.toMillis(10));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        when(processor.getDelaySeconds(anyLong())).thenReturn(-1L);
        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, times(1)).tryRecover(any(), any());
    }

    @Test
    public void testOnEventWithSickWithFullSyncBlockFuture() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(proxyTunnelInfo);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(1).thenReturn(1);
        when(infoResultExtractor.extractAsLong("rdb_last_cow_size")).thenReturn(1024L);

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.delayDownAfterMilli()).thenReturn((int) TimeUnit.SECONDS.toMillis(1));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        when(processor.getDelaySeconds(anyLong())).thenReturn(2L);

        processor.onEvent(new InstanceHalfSick(instance));
        waitConditionUntilTimeOut(()->assertSuccess(()->verify(processor, times(1)).undoDedupe(any())), 5000, 500);
    }

    @Test
    public void testOnEventWithUp() throws HealthEventProcessorException {
        processor.onEvent(new InstanceUp(instance));
        verify(processor, never()).doOnEvent(any());
        verify(processor, never()).tryRecover(any(), any());
    }

    @Test
    public void testOnEventWithDown() throws HealthEventProcessorException {
        processor.onEvent(new InstanceDown(instance));
        verify(processor, never()).doOnEvent(any());
        verify(processor, never()).tryRecover(any(), any());
    }

    @Test
    public void testCloseProxyChain() {
        doCallRealMethod().when(processor).tryRecover(any(), any());
        processor.tryRecover(instance, proxyTunnelInfo);
        verify(proxyManager, times(1)).closeProxyTunnel(proxyTunnelInfo);
    }

    @Test
    public void testCloseProxyChainLimiting() throws InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(proxyTunnelInfo);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(0);

        processor.onEvent(new InstanceHalfSick(instance));
        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, times(1)).tryRecover(any(), any());
    }

    @Test
    public void testCloseProxyChainLimitingRelease() throws InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(proxyTunnelInfo);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(0);

        processor.onEvent(new InstanceHalfSick(instance));
        sleep(200);
        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, times(2)).tryRecover(any(), any());
    }

    @Test
    public void testCloseProxyChainOnSameShardOnlyOnce() throws InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).tryRecover(any(), any());
        when(proxyManager.getProxyTunnelInfo(anyString(), anyString(), anyString(), anyString())).thenReturn(proxyTunnelInfo);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(0);

        RedisHealthCheckInstance instance2 = mock(RedisHealthCheckInstance.class);
        when(instance2.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo("FRA-AWS", "cluster", "shard",
                new HostPort("127.0.0.4", 6380), "SHAJQ", ClusterType.ONE_WAY));
        when(instance2.getRedisSession()).thenReturn(redisSession);
        processor.onEvent(new InstanceHalfSick(instance));
        processor.onEvent(new InstanceHalfSick(instance2));
        verify(processor, times(1)).tryRecover(any(), any());
    }
}