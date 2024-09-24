package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.handler;

import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
public class DefaultOuterClientAggregatorTest {

    @InjectMocks
    private DefaultOuterClientAggregator outerClientAggregator;

    @Mock
    private ScheduledExecutorService scheduled;
    @Mock
    private ExecutorService executors;
    @Mock
    private CheckerConfig checkerConfig;
    @Mock
    private AggregatorPullService aggregatorPullService;

    private final HostPort hostPort1 = new HostPort("127.0.0.1", 6379);

    private final HostPort hostPort2 = new HostPort("127.0.0.1", 6380);

    private final HostPort hostPort3 = new HostPort("127.0.0.1", 6381);

    private final String cluster1 = "cluster1";

    private ClusterShardHostPort info1, info2, info3;

    private int pullIntervalSeconds = 3;

    private int pullRandomSeconds = 3;

    @Before
    public void beforeDefaultOuterClientAggregatorTest() {
        info1 = new ClusterShardHostPort(cluster1, null, hostPort1);
        info2 = new ClusterShardHostPort(cluster1, null, hostPort2);
        info3 = new ClusterShardHostPort(cluster1, null, hostPort3);
        when(checkerConfig.getMarkInstanceBaseDelayMilli()).thenReturn(pullIntervalSeconds * 1000);
        when(checkerConfig.getMarkInstanceMaxDelayMilli()).thenReturn(pullRandomSeconds * 1000);
        outerClientAggregator.setScheduled(MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(1, XpipeThreadFactory.create("DefaultOuterClientAggregatorTest")),
                5, TimeUnit.SECONDS
        ));
    }

    @Test
    public void testRandomMile() {
        for (int i = 0; i < 100000; i++) {
            long randomMill = outerClientAggregator.randomMill();
            assertTrue(randomMill >= pullIntervalSeconds * 1000L && randomMill < (pullIntervalSeconds + pullRandomSeconds) * 1000L);
        }
    }

    @Test
    public void testMarkInstance() {
        outerClientAggregator.markInstance(info1);
        outerClientAggregator.markInstance(info2);
        outerClientAggregator.markInstance(info3);

    }

}
