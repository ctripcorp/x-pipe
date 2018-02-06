package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.health.BaseSamplePlan;
import com.ctrip.xpipe.redis.console.health.Sample;
import com.ctrip.xpipe.redis.console.health.action.AllMonitorCollector;
import com.ctrip.xpipe.redis.console.health.ping.PingService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Feb 01, 2018
 */
public class DefaultDelayMonitorTest2 {

    @Spy
    private DelayCollector collector1 = new AllMonitorCollector();

    @Spy
    private DelayCollector collector2 = new DefaultDelayService();

    @Spy
    private DelayCollector collector3 = new MetricDelayCollector();

    @Mock
    private PingService pingService;

    @InjectMocks
    private DefaultDelayMonitor monitor = new DefaultDelayMonitor();

    private Sample<InstanceDelayResult> sample;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        when(pingService.isRedisAlive(any())).thenReturn(true);
        BaseSamplePlan<InstanceDelayResult> plan = new DelaySamplePlan("cluster", "shard");
        plan.addRedis("dc", new RedisMeta().setMaster("0.0.0.0").setIp("127.0.0.1").setPort(6380),
                new InstanceDelayResult("dc", false));
        plan.addRedis("dc", new RedisMeta().setMaster("0.0.0.0").setIp("127.0.0.1").setPort(6381),
                new InstanceDelayResult("dc", false));
        sample = new Sample<>(System.currentTimeMillis(), System.nanoTime(), plan, 100);
        monitor.setDelayCollectors(Lists.newArrayList(collector1, collector2, collector3));
    }

    @Test
    public void notifyCollectors() throws Exception {
        Mockito.doThrow(new XpipeRuntimeException("Collector 1")).when(collector1).collect(any());
        Mockito.doThrow(new XpipeRuntimeException("Collector 2")).when(collector2).collect(any());
        Mockito.doThrow(new XpipeRuntimeException("Collector 3")).when(collector3).collect(any());

        monitor.notifyCollectors(sample);

        verify(collector1).collect(any());
        verify(collector2).collect(any());
        verify(collector3).collect(any());
    }
}