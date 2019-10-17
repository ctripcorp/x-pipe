package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceHalfSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChain;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ProxyService;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.proxy.monitor.TunnelStatsResult;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RouteHealthEventProcessorTest extends AbstractTest {

    private RouteHealthEventProcessor processor = new RouteHealthEventProcessor();

    @Mock
    private MetaCache metaCache;

    @Mock
    private RedisSessionManager redisSessionManager;

    @Mock
    private ProxyService proxyService;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private RedisSession redisSession;

    @Mock
    private InfoResultExtractor infoResultExtractor;

    private ProxyChain proxyChain;

    private ScheduledExecutorService scheduled;

    @Before
    public void beforeRouteHealthEventProcessorTest() {
        MockitoAnnotations.initMocks(this);
        processor.setMetaCache(metaCache).setProxyService(proxyService).setRedisSessionManager(redisSessionManager);
        scheduled = Executors.newScheduledThreadPool(1);
        processor.setScheduled(scheduled);

        processor = spy(processor);

        //decorate proxy service and proxy chain
        when(proxyService.deleteProxyChain(anyList())).thenReturn(RetMessage.createSuccessMessage());

        List<TunnelInfo> tunnels = Lists.newArrayList(
                new DefaultTunnelInfo(new ProxyModel().setHostPort(new HostPort("127.0.0.1", 80)), "tunnel-1")
                .setTunnelStatsResult(new TunnelStatsResult("", "", -1L, -1L, new HostPort("127.0.0.1", 443), new HostPort("127.0.0.1", 1024))),
                new DefaultTunnelInfo(new ProxyModel().setHostPort(new HostPort("127.0.0.2", 80)), "tunnel-2")
                .setTunnelStatsResult(new TunnelStatsResult("", "", -1L, -1L, new HostPort("127.0.0.2", 443), new HostPort("127.0.0.2", 2048)))
        );
        proxyChain = new DefaultProxyChain("FRA-AWS", "cluster", "shard", tunnels);
        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("FRA-AWS", "cluster", "shard", new HostPort("127.0.0.3", 6379), "SHAJQ"));
        when(instance.getRedisSession()).thenReturn(redisSession);
        when(redisSessionManager.findOrCreateSession(any(HostPort.class))).thenReturn(redisSession);

    }

    @Test
    public void testOnEventWithSickWithNoProxyChain() throws HealthEventProcessorException {
        doNothing().when(processor).closeProxyChain(any(), any());
        when(proxyService.getProxyChain(anyString(), anyString(), anyString())).thenReturn(null);

        processor.onEvent(new InstanceHalfSick(instance));

        verify(processor, never()).closeProxyChain(any(), any());
    }

    @Test
    public void testOnEventWithSickWithPartialSync() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).closeProxyChain(any(), any());
        when(proxyService.getProxyChain(anyString(), anyString(), anyString())).thenReturn(proxyChain);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(0);

        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, times(1)).closeProxyChain(any(), any());
    }

    @Test
    public void testOnEventWithSickWithFullSyncBlockNow() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).closeProxyChain(any(), any());
        when(proxyService.getProxyChain(anyString(), anyString(), anyString())).thenReturn(proxyChain);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(1);
        when(infoResultExtractor.extractAsLong("rdb_last_cow_size")).thenReturn(1024L);

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.delayDownAfterMilli()).thenReturn((int) TimeUnit.MINUTES.toMillis(10));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        when(processor.getDelaySeconds(anyLong())).thenReturn(-1L);
        processor.onEvent(new InstanceHalfSick(instance));
        verify(processor, times(1)).closeProxyChain(any(), any());
    }

    @Test
    public void testOnEventWithSickWithFullSyncBlockFuture() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).closeProxyChain(any(), any());
        when(proxyService.getProxyChain(anyString(), anyString(), anyString())).thenReturn(proxyChain);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(1).thenReturn(1);
        when(infoResultExtractor.extractAsLong("rdb_last_cow_size")).thenReturn(1024L);

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.delayDownAfterMilli()).thenReturn((int) TimeUnit.SECONDS.toMillis(1));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        when(processor.getDelaySeconds(anyLong())).thenReturn(2L);

        processor.onEvent(new InstanceHalfSick(instance));

        Thread.sleep(1000);
        verify(processor, atLeast(2)).isRedisInFullSync(any());
        verify(processor, times(1)).closeProxyChain(any(), any());
    }

    @Test
    public void testOnEventWithSickWithFullSyncNonBlock() throws HealthEventProcessorException, InterruptedException, ExecutionException, TimeoutException {
        doNothing().when(processor).closeProxyChain(any(), any());
        when(proxyService.getProxyChain(anyString(), anyString(), anyString())).thenReturn(proxyChain);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.REPLICATION)).thenReturn(infoResultExtractor);
        when(redisSession.syncInfo(InfoCommand.INFO_TYPE.PERSISTENCE)).thenReturn(infoResultExtractor);
        when(infoResultExtractor.extractAsInteger("master_sync_in_progress")).thenReturn(1).thenReturn(0);
        when(infoResultExtractor.extractAsLong("rdb_last_cow_size")).thenReturn(1024L);

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.delayDownAfterMilli()).thenReturn((int) TimeUnit.SECONDS.toMillis(1));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        when(processor.getDelaySeconds(anyLong())).thenReturn(2L);

        processor.onEvent(new InstanceHalfSick(instance));

        Thread.sleep(1000);
        verify(processor, atLeast(2)).isRedisInFullSync(any());
        verify(processor, never()).closeProxyChain(any(), any());
    }

    @Test
    public void testOnEventWithUp() throws HealthEventProcessorException {
        processor.onEvent(new InstanceUp(instance));
        verify(processor, never()).doOnEvent(any());
        verify(processor, never()).closeProxyChain(any(), any());
    }

    @Test
    public void testOnEventWithDown() throws HealthEventProcessorException {
        processor.onEvent(new InstanceDown(instance));
        verify(processor, never()).doOnEvent(any());
        verify(processor, never()).closeProxyChain(any(), any());
    }

    @Test
    public void testCloseProxyChain() {
        doCallRealMethod().when(processor).closeProxyChain(any(), any());
        processor.closeProxyChain(instance, proxyChain);
        verify(proxyService, times(1)).deleteProxyChain(anyList());
    }
}