package com.ctrip.xpipe.redis.console.healthcheck.action;

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
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.action.handler.*;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.tuple.Pair;
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
    protected DelayPingActionListener delayPingActionListener;

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
        when(checker.check(any())).thenReturn(future);
        HostPort master = new HostPort();
        when(delayPingActionListener.getState(master)).thenReturn(HEALTH_STATE.UP);
        when(metaCache.findMasterInSameShard(any())).thenReturn(master);
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(true);
        when(consoleServiceManager.allHealthStatus(anyString(), anyInt())).thenReturn(
                Lists.newArrayList(HEALTH_STATE.SICK, HEALTH_STATE.DOWN, HEALTH_STATE.UNHEALTHY));
        when(consoleServiceManager.quorumSatisfy(anyList(), any())).thenReturn(true);
        processor.setEventHandlers(Lists.newArrayList(instanceSickHandler, instanceDownHandler, instanceUpHandler));

        instance = mock(RedisHealthCheckInstance.class);
        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(dc, cluster, shard, hostPort));

        FinalStateSetterManager<ClusterShardHostPort, Boolean> manager = mock(FinalStateSetterManager.class);
        ((AbstractHealthEventHandler)instanceUpHandler).setFinalStateSetterManager(manager);
        ((AbstractHealthEventHandler)instanceDownHandler).setFinalStateSetterManager(manager);
        ((AbstractHealthEventHandler)instanceSickHandler).setFinalStateSetterManager(manager);
    }

    @Test
    public void testOnEventConfiguredNotMarkDown() throws HealthEventProcessorException {

        when(consoleConfig.getDelayWontMarkDownClusters()).thenReturn(Sets.newHashSet(new Pair<>(dc, cluster)));
        when(metaCache.inBackupDc(hostPort)).thenReturn(true);
        when(metaCache.getRedisNumOfDc(anyString())).thenReturn(10);
        processor.onEvent(new InstanceSick(instance));
        verify(alertManager, atLeastOnce()).alert(cluster, shard, hostPort, ALERT_TYPE.INSTANCE_SICK_BUT_NOT_MARK_DOWN, dc);
    }

    @Test
    public void testHandleExecute() {
        instanceDownHandler = spy(instanceDownHandler);
        instanceSickHandler = spy(instanceSickHandler);
        instanceUpHandler = spy(instanceUpHandler);
        processor.onEvent(new InstanceSick(instance));
        verify(instanceDownHandler, never()).handle(any());
        verify(instanceUpHandler, never()).handle(any());
    }

}