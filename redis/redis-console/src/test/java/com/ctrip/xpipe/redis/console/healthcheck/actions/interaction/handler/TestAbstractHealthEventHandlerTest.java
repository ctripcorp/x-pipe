package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.SiteReliabilityChecker;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceUp;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
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
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private SiteReliabilityChecker checker;

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
        when(instance.getRedisInstanceInfo()).thenReturn(info);

        when(checker.isSiteHealthy(any(AbstractInstanceEvent.class))).thenReturn(true);
        when(defaultDelayPingActionCollector.getState(any())).thenReturn(HEALTH_STATE.DOWN);
        when(defaultDelayPingActionCollector.getHealthStateSetterManager()).thenReturn(finalStateSetterManager);
        doNothing().when(finalStateSetterManager).set(any(ClusterShardHostPort.class), anyBoolean());
        ((DefaultInstanceSickHandler) sickHandler).setScheduled(Executors.newScheduledThreadPool(1));
    }

    @Test
    public void testMarkDown() {
        when(checker.isSiteHealthy(any(AbstractInstanceEvent.class))).thenReturn(false);
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, never()).set(any(ClusterShardHostPort.class), anyBoolean());

        when(checker.isSiteHealthy(any(AbstractInstanceEvent.class))).thenReturn(true);
        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        when(consoleConfig.getDelayedMarkDownDcClusters()).thenReturn(Sets.newHashSet(new DcClusterDelayMarkDown()
                .setDcId(info.getDcId()).setClusterId(info.getClusterId()).setDelaySecond(1)));
        sickHandler.markdown(new InstanceSick(instance));
        sleep(1500);
        verify(finalStateSetterManager, times(1)).set(any(), any());

        when(consoleConfig.getDelayedMarkDownDcClusters()).thenReturn(null);
        when(checker.isSiteHealthy(any(AbstractInstanceEvent.class))).thenReturn(true);
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, times(2)).set(any(ClusterShardHostPort.class), anyBoolean());
    }

    @Test
    public void testMarkDownForInstanceDown() {
        when(checker.isSiteHealthy(any(AbstractInstanceEvent.class))).thenReturn(false);
        downHandler.markdown(new InstanceDown(instance));
        verify(finalStateSetterManager, never()).set(any(), anyBoolean());

        when(checker.isSiteHealthy(any(AbstractInstanceEvent.class))).thenReturn(true);

        downHandler.markdown(new InstanceDown(instance));
        verify(finalStateSetterManager, times(1)).set(any(), anyBoolean());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHandle() {
        when(consoleConfig.getDelayedMarkDownDcClusters()).thenReturn(null);
        when(metaCache.inBackupDc(any())).thenReturn(true);
        future.setSuccess(true);
        when(consoleServiceManager.quorumSatisfy(anyList(), any())).thenReturn(true);
        when(defaultDelayPingActionCollector.getState(any())).thenReturn(HEALTH_STATE.HEALTHY);
        when(defaultDelayPingActionCollector.getState(instance.getRedisInstanceInfo().getHostPort())).thenReturn(HEALTH_STATE.DOWN);

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

        event = new InstanceDown(instance);
        upHandler.handle(event);
        downHandler.handle(event);
        sickHandler.handle(event);
        verify(finalStateSetterManager, times(3)).set(any(ClusterShardHostPort.class), anyBoolean());
    }

    private RedisHealthCheckInstance randomInstance(String dc) {
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        RedisInstanceInfo info = new DefaultRedisInstanceInfo(dc, "cluster", "shard", localHostport(randomPort()), "dc2", ClusterType.ONE_WAY);
        when(instance.getRedisInstanceInfo()).thenReturn(info);
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