package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler.*;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 08, 2018
 */
public class OuterClientServiceProcessorTest extends AbstractRedisTest {

    @InjectMocks
    private OuterClientServiceProcessor processor = new OuterClientServiceProcessor();

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private RemoteCheckerManager remoteCheckerManager;

    @Mock
    protected DefaultDelayPingActionCollector defaultDelayPingActionCollector;

    @Mock
    private StabilityHolder siteStability;

    @InjectMocks
    private InstanceSickHandler instanceSickHandler = new DefaultInstanceSickHandler();

    @InjectMocks
    private InstanceDownHandler instanceDownHandler = new DefaultInstanceDownHandler();

    @InjectMocks
    private InstanceUpHandler instanceUpHandler = new DefaultInstanceUpHandler();

    private RedisHealthCheckInstance instance;


    private String dc = "dc", cluster = "cluster", shard = "shard";

    private HostPort hostPort = localHostport(randomPort());

    @SuppressWarnings("unchecked")
    @Before
    public void beforeOuterClientServiceProcessorTest() {
        MockitoAnnotations.initMocks(this);
        CommandFuture<Boolean> future = new DefaultCommandFuture<>();
        future.setSuccess(true);
        when(siteStability.isSiteStable()).thenReturn(true);
        when(defaultDelayPingActionCollector.getState(any())).thenReturn(HEALTH_STATE.DOWN);
        HostPort master = localHostport(randomPort());
        when(defaultDelayPingActionCollector.getState(master)).thenReturn(HEALTH_STATE.HEALTHY);
        when(metaCache.findMasterInSameShard(any())).thenReturn(master);
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(true);
        when(remoteCheckerManager.getHealthStates(anyString(), anyInt())).thenReturn(Lists.newArrayList(HEALTH_STATE.SICK, HEALTH_STATE.DOWN, HEALTH_STATE.UNHEALTHY));
        when(checkerConfig.getQuorum()).thenReturn(1);
        processor.setEventHandlers(Lists.newArrayList(instanceSickHandler, instanceDownHandler, instanceUpHandler));

        instance = mock(RedisHealthCheckInstance.class);
        when(instance.getCheckInfo()).thenReturn(new DefaultRedisInstanceInfo(dc, cluster, shard, hostPort, dc, ClusterType.ONE_WAY));

        HealthCheckConfig config = mock(HealthCheckConfig.class);
        when(config.getDelayConfig(any(), any(), any())).thenReturn(new DelayConfig("test", "test", "test").
                setClusterLevelDelayDownAfterMilli(24000).setClusterLevelHealthyDelayMilli(2000));
        when(instance.getHealthCheckConfig()).thenReturn(config);

        FinalStateSetterManager<ClusterShardHostPort, Boolean> manager = mock(FinalStateSetterManager.class);
        when(defaultDelayPingActionCollector.getHealthStateSetterManager()).thenReturn(manager);
        ((DefaultInstanceSickHandler)instanceSickHandler).setScheduled(scheduled);
    }

    @Test
    public void testHandleExecute() {
        instanceDownHandler = spy(instanceDownHandler);
        instanceSickHandler = spy(instanceSickHandler);
        instanceUpHandler = spy(instanceUpHandler);
        processor.setEventHandlers(Lists.newArrayList(instanceUpHandler, instanceDownHandler, instanceSickHandler));
        processor.onEvent(new InstanceSick(instance));
        verify(instanceDownHandler, times(1)).handle(any());
        verify(instanceUpHandler, times(1)).handle(any());
        verify(instanceSickHandler, times(1)).handle(any());
    }

}