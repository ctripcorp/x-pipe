package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.handler.*;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
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
    private ConsoleConfig consoleConfig;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private SiteReliabilityChecker checker;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    protected DefaultDelayPingActionCollector defaultDelayPingActionCollector;

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
        when(checker.isSiteHealthy(any())).thenReturn(true);
        when(defaultDelayPingActionCollector.getState(any())).thenReturn(HEALTH_STATE.DOWN);
        HostPort master = localHostport(randomPort());
        when(defaultDelayPingActionCollector.getState(master)).thenReturn(HEALTH_STATE.HEALTHY);
        when(metaCache.findMasterInSameShard(any())).thenReturn(master);
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(true);
        when(consoleServiceManager.allHealthStatus(anyString(), anyInt())).thenReturn(
                Lists.newArrayList(HEALTH_STATE.SICK, HEALTH_STATE.DOWN, HEALTH_STATE.UNHEALTHY));
        when(consoleServiceManager.quorumSatisfy(anyList(), any())).thenReturn(true);
        processor.setEventHandlers(Lists.newArrayList(instanceSickHandler, instanceDownHandler, instanceUpHandler));

        instance = mock(RedisHealthCheckInstance.class);
        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(dc, cluster, shard, hostPort, dc));

        FinalStateSetterManager<ClusterShardHostPort, Boolean> manager = mock(FinalStateSetterManager.class);
        when(defaultDelayPingActionCollector.getHealthStateSetterManager()).thenReturn(manager);
        ((DefaultInstanceSickHandler)instanceSickHandler).setScheduled(scheduled);
    }

    @Test
    public void testOnEventConfiguredNotMarkDown() throws HealthEventProcessorException {

        when(consoleConfig.getDelayedMarkDownDcClusters()).thenReturn(
                Sets.newHashSet(new DcClusterDelayMarkDown().setDcId(dc).setClusterId(cluster)));
        when(metaCache.inBackupDc(hostPort)).thenReturn(true);
        when(checker.isSiteHealthy(any())).thenReturn(true);
        processor.onEvent(new InstanceSick(instance));
        verify(alertManager, atLeastOnce()).alert(instance.getRedisInstanceInfo(), ALERT_TYPE.INSTANCE_SICK_BUT_DELAY_MARK_DOWN, dc);
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