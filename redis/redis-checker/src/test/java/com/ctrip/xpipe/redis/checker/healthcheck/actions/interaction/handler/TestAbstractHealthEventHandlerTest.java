package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.*;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Random;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 19, 2018
 */
public class TestAbstractHealthEventHandlerTest extends AbstractRedisTest {

    @InjectMocks
    private AbstractHealthEventHandler sickHandler = new DefaultInstanceSickHandler();

    @InjectMocks
    private AbstractHealthEventHandler downHandler = new DefaultInstanceDownHandler();

    @InjectMocks
    private AbstractHealthEventHandler upHandler = new DefaultInstanceUpHandler();

    @Mock
    private MetaCache metaCache;

    @Mock
    protected AlertManager alertManager;

    @Mock
    protected DefaultDelayPingActionCollector defaultDelayPingActionCollector;

    @Mock
    private RemoteCheckerManager remoteCheckerManager;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private StabilityHolder siteStability;

    @Mock
    private FinalStateSetterManager<ClusterShardHostPort, Boolean> finalStateSetterManager;

    private RedisHealthCheckInstance instance;

    private CommandFuture<Boolean> future = new DefaultCommandFuture<>();

    private Random random = new Random();

    @Before
    public void beforeTestAbstractHealthEventHandlerTest() {
        MockitoAnnotations.initMocks(this);

        instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", localHostport(randomPort()), "dc2", ClusterType.ONE_WAY);
        when(instance.getCheckInfo()).thenReturn(info);

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.getDelayConfig(any(), any(), any())).thenReturn(
                new DelayConfig("test", "test", "test")
                        .setClusterLevelHealthyDelayMilli(2000).setClusterLevelDelayDownAfterMilli(24000));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        when(siteStability.isSiteStable()).thenReturn(true);
        when(defaultDelayPingActionCollector.getState(any())).thenReturn(HEALTH_STATE.DOWN);
        when(defaultDelayPingActionCollector.getHealthStateSetterManager()).thenReturn(finalStateSetterManager);
        doNothing().when(finalStateSetterManager).set(any(ClusterShardHostPort.class), anyBoolean());
        ((DefaultInstanceSickHandler) sickHandler).setScheduled(Executors.newScheduledThreadPool(1));
    }

    @Test
    public void testTryMarkDown() {
        HostPort master = new HostPort("master",1111);
        when(metaCache.findMasterInSameShard(any())).thenReturn(master);
        doAnswer(inv -> {
            HostPort hostPort = inv.getArgument(0);
            if (hostPort.equals(master)) return HEALTH_STATE.HEALTHY;
            else return HEALTH_STATE.DOWN;
        }).when(defaultDelayPingActionCollector).getState(any());

        when(siteStability.isSiteStable()).thenReturn(false);
        downHandler.tryMarkDown(new InstanceLoading(instance));
        verify(finalStateSetterManager, never()).set(any(), anyBoolean());

        when(siteStability.isSiteStable()).thenReturn(true);
        downHandler.tryMarkDown(new InstanceLoading(instance));
        verify(finalStateSetterManager, times(1)).set(any(), anyBoolean());
    }

    @Test
    public void testMarkDown() {
        when(siteStability.isSiteStable()).thenReturn(false);
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, never()).set(any(ClusterShardHostPort.class), anyBoolean());

        when(siteStability.isSiteStable()).thenReturn(true);
        RedisInstanceInfo info = instance.getCheckInfo();
        when(checkerConfig.getDelayedMarkDownDcClusters()).thenReturn(Sets.newHashSet(new DcClusterDelayMarkDown()
                .setDcId(info.getDcId()).setClusterId(info.getClusterId()).setDelaySecond(1)));
        sickHandler.markdown(new InstanceSick(instance));
        sleep(1500);
        verify(finalStateSetterManager, times(1)).set(any(), any());

        when(checkerConfig.getDelayedMarkDownDcClusters()).thenReturn(null);
        when(siteStability.isSiteStable()).thenReturn(true);
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, times(2)).set(any(ClusterShardHostPort.class), anyBoolean());
    }

    @Test
    public void testMarkDownForInstanceDown() {
        when(siteStability.isSiteStable()).thenReturn(false);
        downHandler.markdown(new InstanceDown(instance));
        verify(finalStateSetterManager, never()).set(any(), anyBoolean());

        when(siteStability.isSiteStable()).thenReturn(true);

        downHandler.markdown(new InstanceDown(instance));
        verify(finalStateSetterManager, times(1)).set(any(), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHandle() {
        when(checkerConfig.getDelayedMarkDownDcClusters()).thenReturn(null);
        when(checkerConfig.getQuorum()).thenReturn(0);
        when(metaCache.inBackupDc(any())).thenReturn(true);
        future.setSuccess(true);
        when(defaultDelayPingActionCollector.getState(any())).thenReturn(HEALTH_STATE.HEALTHY);
        when(defaultDelayPingActionCollector.getState(instance.getCheckInfo().getHostPort())).thenReturn(HEALTH_STATE.DOWN);

        AbstractInstanceEvent event = new InstanceUp(instance);
        upHandler.handle(event);
        downHandler.handle(event);
        sickHandler.handle(event);
        verify(finalStateSetterManager, times(1)).set(any(ClusterShardHostPort.class), anyBoolean());

        event = new InstanceSick(instance);
        upHandler.handle(event);
        downHandler.handle(event);
        sickHandler.handle(event);
        verify(finalStateSetterManager, times(2)).set(any(ClusterShardHostPort.class), anyBoolean());

        //do not markdown cross region instance
        DefaultRedisInstanceInfo instanceInfo= (DefaultRedisInstanceInfo) instance.getCheckInfo();
        instanceInfo.setCrossRegion(true);
        upHandler.handle(event);
        downHandler.handle(event);
        sickHandler.handle(event);
        verify(finalStateSetterManager, times(2)).set(any(ClusterShardHostPort.class), anyBoolean());

        //do not markdown instance which dcs distance is -1
        instanceInfo.setCrossRegion(false);
        HealthCheckConfig config = instance.getHealthCheckConfig();
        when(config.getDelayConfig(any(), any(), any())).thenReturn(
                new DelayConfig("test", "test", "test")
                        .setClusterLevelHealthyDelayMilli(-2000).setClusterLevelDelayDownAfterMilli(-24000));
        upHandler.handle(event);
        downHandler.handle(event);
        sickHandler.handle(event);
        verify(finalStateSetterManager, times(2)).set(any(ClusterShardHostPort.class), anyBoolean());

        event = new InstanceDown(instance);
        upHandler.handle(event);
        downHandler.handle(event);
        sickHandler.handle(event);
        verify(finalStateSetterManager, times(3)).set(any(ClusterShardHostPort.class), anyBoolean());
    }

    private RedisHealthCheckInstance randomInstance(String dc) {
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = new DefaultRedisInstanceInfo(dc, "cluster", "shard", localHostport(randomPort()), "dc2", ClusterType.ONE_WAY);
        when(instance.getCheckInfo()).thenReturn(info);
        return instance;
    }

    private RedisHealthCheckInstance randomInstance() {
        return randomInstance(randomDc());
    }

    private String randomDc() {
        int index = Math.abs(random.nextInt() % 4);
        return "dc" + index;
    }
}