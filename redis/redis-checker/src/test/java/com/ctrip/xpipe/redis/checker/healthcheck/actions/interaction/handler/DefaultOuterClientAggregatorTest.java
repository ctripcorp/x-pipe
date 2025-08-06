package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.ClusterActiveDcKey;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultOuterClientAggregatorTest extends AbstractTest {

    @Spy
    @InjectMocks
    private DefaultOuterClientAggregator outerClientAggregator;

    @Mock
    private CheckerConfig checkerConfig;
    @Mock
    private AggregatorPullService aggregatorPullService;
    @Mock
    private HealthStateService healthStateService;

    private final HostPort hostPort1 = new HostPort("127.0.0.1", 6379);

    private final HostPort hostPort2 = new HostPort("127.0.0.1", 6380);

    private final HostPort hostPort3 = new HostPort("127.0.0.1", 6381);

    private final String cluster1 = "cluster1";

    private final String activeDc = "jq";

    private final String backupDc = "oy";

    private ClusterShardHostPort info1, info2, info3;

    private int baseDelay = 3;

    private int maxDelay = 6;

    @Before
    public void beforeDefaultOuterClientAggregatorTest() {
        info1 = new ClusterShardHostPort(cluster1, null, activeDc, hostPort1);
        info2 = new ClusterShardHostPort(cluster1, null, activeDc, hostPort2);
        info3 = new ClusterShardHostPort(cluster1, null, activeDc, hostPort3);
        when(checkerConfig.getMarkInstanceBaseDelayMilli()).thenReturn(baseDelay * 1000);
        when(checkerConfig.getMarkdownInstanceMaxDelayMilli()).thenReturn(maxDelay * 1000);
        outerClientAggregator.setScheduled(MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create("DefaultOuterClientAggregatorTest")),
                5, TimeUnit.SECONDS
        ));
        outerClientAggregator.setHealthStateServices(Collections.singletonList(healthStateService));
        outerClientAggregator.setExecutors(Executors.newFixedThreadPool(5));
        outerClientAggregator.postConstruct();
    }

    @Test
    public void testAggregateMarkdown() throws Exception {
        doReturn(10).when(outerClientAggregator).randomMill();
        doReturn(200).when(outerClientAggregator).checkInterval();
        CountDownLatch latch = new CountDownLatch(3);
        executors.execute(() -> {
            outerClientAggregator.markInstance(info1);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info2);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info3);
            latch.countDown();
        });
        latch.await();
        DefaultOuterClientAggregator.Aggregator aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertNotNull(aggregator);
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());

        doAnswer(invocation -> {
            long callCount = Mockito.mockingDetails(aggregatorPullService)
                    .getInvocations()
                    .stream()
                    .filter(inv -> inv.getMethod().getName().equals("getNeedAdjustInstances"))
                    .count();
            if (callCount == 1) {
                Set<HostPort> instances = (Set<HostPort>) invocation.getArguments()[1];
                return instances.stream().map(instance -> new OuterClientService.HostPortDcStatus(instance.getHost(), instance.getPort(), backupDc, false)).collect(Collectors.toSet());
            } else {
                return new HashSet<>();
            }
        }).when(aggregatorPullService).getNeedAdjustInstances(anyString(), anySet());

        Thread.sleep(500);
        verify(healthStateService, times(3)).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, times(1)).doMarkInstances(anyString(), anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertNotNull(aggregator);
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertTrue(aggregator.getDoing().isEmpty());
        Assert.assertEquals(0, aggregator.getWaitStartTime());
    }

    @Test
    public void testAggregateMarkdownAndMarkUp() throws Exception {
        doReturn(10).when(outerClientAggregator).randomMill();
        doReturn(200).when(outerClientAggregator).checkInterval();
        CountDownLatch latch = new CountDownLatch(3);
        executors.execute(() -> {
            outerClientAggregator.markInstance(info1);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info2);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info3);
            latch.countDown();
        });
        latch.await();
        DefaultOuterClientAggregator.Aggregator aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertNotNull(aggregator);
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());

        doAnswer(invocation -> {
            long callCount = Mockito.mockingDetails(aggregatorPullService)
                    .getInvocations()
                    .stream()
                    .filter(inv -> inv.getMethod().getName().equals("getNeedAdjustInstances"))
                    .count();
            if (callCount == 1) {
                Set<HostPort> instances = (Set<HostPort>) invocation.getArguments()[1];
                Set<OuterClientService.HostPortDcStatus> hostPortDcStatuses = new HashSet<>();
                instances.forEach(hostPort -> {
                    if (hostPort.getPort() == 6379) {
                        hostPortDcStatuses.add(new OuterClientService.HostPortDcStatus(hostPort.getHost(), hostPort.getPort(), backupDc, false));
                    } else {
                        hostPortDcStatuses.add(new OuterClientService.HostPortDcStatus(hostPort.getHost(), hostPort.getPort(), backupDc, true));
                    }
                });
                return hostPortDcStatuses;
            } else {
                return new HashSet<>();
            }
        }).when(aggregatorPullService).getNeedAdjustInstances(anyString(), anySet());

        Thread.sleep(500);
        verify(healthStateService, times(3)).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, times(1)).doMarkInstances(anyString(), anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertNotNull(aggregator);
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertTrue(aggregator.getDoing().isEmpty());
        Assert.assertEquals(0, aggregator.getWaitStartTime());
    }

    @Test
    public void testAggregateMarkUpWaitTimeout() throws Exception {
        doReturn(10).when(outerClientAggregator).randomMill();
        doReturn(200).when(outerClientAggregator).checkInterval();
        when(checkerConfig.getMarkupInstanceMaxDelayMilli()).thenReturn(800);
        CountDownLatch latch = new CountDownLatch(3);
        executors.execute(() -> {
            outerClientAggregator.markInstance(info1);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info2);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info3);
            latch.countDown();
        });
        latch.await();
        DefaultOuterClientAggregator.Aggregator aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertNotNull(aggregator);
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());

        doAnswer(invocation -> {
            Set<HostPort> instances = (Set<HostPort>) invocation.getArguments()[1];
            return instances.stream().map(instance -> new OuterClientService.HostPortDcStatus(instance.getHost(), instance.getPort(), backupDc, true)).collect(Collectors.toSet());
        }).when(aggregatorPullService).getNeedAdjustInstances(anyString(), anySet());

        Thread.sleep(400);
        verify(aggregatorPullService, atLeast(1)).getNeedAdjustInstances(anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertFalse(aggregator.getDoing().isEmpty());
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());
        verify(healthStateService, never()).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, never()).doMarkInstances(anyString(), anyString(), anySet());

        Thread.sleep(500);
        verify(aggregatorPullService, atLeast(2)).getNeedAdjustInstances(anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertTrue(aggregator.getDoing().isEmpty());
        Assert.assertEquals(0, aggregator.getWaitStartTime());
        verify(healthStateService, times(3)).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, times(1)).doMarkInstances(anyString(), anyString(), anySet());
    }

    @Test
    public void testAggregateMarkUpDcInstancesAllUp() throws Exception {
        doReturn(10).when(outerClientAggregator).randomMill();
        doReturn(200).when(outerClientAggregator).checkInterval();
        doReturn(false).when(outerClientAggregator).dcInstancesAllUp(anyString(), anyString(), any());
        when(checkerConfig.getMarkupInstanceMaxDelayMilli()).thenReturn(8000);
        CountDownLatch latch = new CountDownLatch(3);
        executors.execute(() -> {
            outerClientAggregator.markInstance(info1);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info2);
            latch.countDown();
        });
        executors.execute(() -> {
            outerClientAggregator.markInstance(info3);
            latch.countDown();
        });
        latch.await();
        DefaultOuterClientAggregator.Aggregator aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertNotNull(aggregator);
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());

        doAnswer(invocation -> {
            Set<HostPort> instances = (Set<HostPort>) invocation.getArguments()[1];
            return instances.stream().map(instance -> new OuterClientService.HostPortDcStatus(instance.getHost(), instance.getPort(), backupDc, true)).collect(Collectors.toSet());
        }).when(aggregatorPullService).getNeedAdjustInstances(anyString(), anySet());

        Thread.sleep(400);
        verify(aggregatorPullService, atLeast(1)).getNeedAdjustInstances(anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertFalse(aggregator.getDoing().isEmpty());
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());
        verify(healthStateService, never()).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, never()).doMarkInstances(anyString(), anyString(), anySet());

        Thread.sleep(500);
        verify(aggregatorPullService, atLeast(2)).getNeedAdjustInstances(anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertFalse(aggregator.getDoing().isEmpty());
        Assert.assertNotEquals(0, aggregator.getWaitStartTime());
        verify(healthStateService, never()).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, never()).doMarkInstances(anyString(), anyString(), anySet());

        doReturn(true).when(outerClientAggregator).dcInstancesAllUp(anyString(), anyString(), any());
        Thread.sleep(500);
        verify(aggregatorPullService, atLeast(3)).getNeedAdjustInstances(anyString(), anySet());
        aggregator = outerClientAggregator.getClusterAggregator(new ClusterActiveDcKey(cluster1, activeDc));
        Assert.assertTrue(aggregator.getTodo().isEmpty());
        Assert.assertTrue(aggregator.getDoing().isEmpty());
        Assert.assertEquals(0, aggregator.getWaitStartTime());
        verify(healthStateService, times(3)).updateLastMarkHandled(any(), anyBoolean());
        verify(aggregatorPullService, times(1)).doMarkInstances(anyString(), anyString(), anySet());
    }

}
