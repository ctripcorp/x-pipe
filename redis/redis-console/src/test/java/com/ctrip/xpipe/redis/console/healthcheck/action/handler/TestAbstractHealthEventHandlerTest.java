package com.ctrip.xpipe.redis.console.healthcheck.action.handler;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.concurrent.FinalStateSetterManager;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.action.*;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.AbstractInstanceEvent;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceDown;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceSick;
import com.ctrip.xpipe.redis.console.healthcheck.action.event.InstanceUp;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Random;

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
    protected DelayPingActionListener delayPingActionListener;

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
        RedisInstanceInfo info = new DefaultRedisInstanceInfo("dc", "cluster", "shard", localHostport(randomPort()));
        when(instance.getRedisInstanceInfo()).thenReturn(info);

        when(checker.check(any(AbstractInstanceEvent.class))).thenReturn(future);
        doNothing().when(finalStateSetterManager).set(any(ClusterShardHostPort.class), anyBoolean());
    }

    @Test
    public void testMarkDown() {
        future.setSuccess(false);
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, never()).set(any(ClusterShardHostPort.class), anyBoolean());

        CommandFuture<Boolean> future = new DefaultCommandFuture<>();
        future.setSuccess(true);

        RedisInstanceInfo info = instance.getRedisInstanceInfo();
        when(consoleConfig.getDelayWontMarkDownClusters()).thenReturn(Sets.newHashSet(new Pair<>(info.getDcId(), info.getClusterId())));
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, never()).set(any(), any());

        when(consoleConfig.getDelayWontMarkDownClusters()).thenReturn(null);
        when(checker.check(any(AbstractInstanceEvent.class))).thenReturn(future);
        sickHandler.markdown(new InstanceSick(instance));
        verify(finalStateSetterManager, times(1)).set(any(ClusterShardHostPort.class), anyBoolean());
    }

    @Test
    public void testMarkDownForInstanceDown() {
        future.setSuccess(false);
        downHandler.markdown(new InstanceDown(instance));
        verify(finalStateSetterManager, never()).set(any(), anyBoolean());

        CommandFuture<Boolean> future = new DefaultCommandFuture<>();
        future.setSuccess(true);
        when(checker.check(any(AbstractInstanceEvent.class))).thenReturn(future);

        downHandler.markdown(new InstanceDown(instance));
        verify(finalStateSetterManager, times(1)).set(any(), anyBoolean());
    }

    @Test
    public void testMarkDownMultiple() {
        int N = 100;
        DefaultSiteReliabilityChecker checker = new DefaultSiteReliabilityChecker();
        checker.setScheduled(scheduled);
        checker.setMetaCache(metaCache);
        when(metaCache.getRedisNumOfDc(anyString())).thenReturn(N);

        downHandler.setChecker(checker);
        sickHandler.setChecker(checker);

        HealthStatus.PING_DOWN_AFTER_MILLI = 30 * 5;
        int sleepInterval = HealthStatus.PING_DOWN_AFTER_MILLI / 5 + 10;
        for(int i = 0; i < N/4; i++) {
            downHandler.markdown(new InstanceDown(randomInstance("dc0")));
            sickHandler.markdown(new InstanceSick(randomInstance("dc0")));
        }
        sleep(sleepInterval);

        verify(finalStateSetterManager, never()).set(any(), anyBoolean());

        logger.info("=================================== I'm the splitter================================");
        sleep(sleepInterval);
        for(int i = 0; i < N/4; i++) {
            downHandler.markdown(new InstanceDown(randomInstance()));
            sickHandler.markdown(new InstanceSick(randomInstance()));
        }
        sleep(sleepInterval * 2);
        verify(finalStateSetterManager, atLeast(1)).set(any(ClusterShardHostPort.class), anyBoolean());
        sleep(1000);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testHandle() {
        when(consoleConfig.getDelayWontMarkDownClusters()).thenReturn(null);
        when(metaCache.inBackupDc(any())).thenReturn(true);
        future.setSuccess(true);
        when(consoleServiceManager.quorumSatisfy(anyList(), any())).thenReturn(true);
        when(delayPingActionListener.getState(any())).thenReturn(HEALTH_STATE.UP);

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
        RedisInstanceInfo info = new DefaultRedisInstanceInfo(dc, "cluster", "shard", localHostport(randomPort()));
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