package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultOuterClientAggregatorTest {

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

    private ClusterShardHostPort info1, info2, info3;

    private int baseDelay = 3;

    private int maxDelay = 6;

    @Before
    public void beforeDefaultOuterClientAggregatorTest() {
        info1 = new ClusterShardHostPort(cluster1, null, hostPort1);
        info2 = new ClusterShardHostPort(cluster1, null, hostPort2);
        info3 = new ClusterShardHostPort(cluster1, null, hostPort3);
        when(checkerConfig.getMarkInstanceBaseDelayMilli()).thenReturn(baseDelay * 1000);
        when(checkerConfig.getMarkInstanceMaxDelayMilli()).thenReturn(maxDelay * 1000);
        outerClientAggregator.setScheduled(MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create("DefaultOuterClientAggregatorTest")),
                5, TimeUnit.SECONDS
        ));
        outerClientAggregator.setHealthStateServices(Collections.singletonList(healthStateService));
    }

    @Test
    public void testMarkInstance() {
        outerClientAggregator.markInstance(info1);
        outerClientAggregator.markInstance(info2);
        outerClientAggregator.markInstance(info3);
    }

    @Test
    public void testAggregate() throws Exception {
        Set<OuterClientService.HostPortDcStatus> toMarkInstances = Sets.newHashSet(
                new OuterClientService.HostPortDcStatus(hostPort1.getHost(), hostPort1.getPort(), "jq", true),
                new OuterClientService.HostPortDcStatus(hostPort2.getHost(), hostPort2.getPort(), "jq", false)
        );
        when(aggregatorPullService.getNeedAdjustInstances(anyString(), anySet())).thenReturn(toMarkInstances);

        DefaultOuterClientAggregator.AggregatorCheckAndSetTask aggregateTask =
                outerClientAggregator.new AggregatorCheckAndSetTask(cluster1, Sets.newHashSet(hostPort1, hostPort2, hostPort3));
        aggregateTask.execute().get();

        Mockito.verify(healthStateService, times(2)).updateLastMarkHandled(any(), anyBoolean());
        Mockito.verify(healthStateService).updateLastMarkHandled(hostPort1, true);
        Mockito.verify(healthStateService).updateLastMarkHandled(hostPort2, false);

        Mockito.verify(aggregatorPullService).doMarkInstances(cluster1, toMarkInstances);
    }

}
