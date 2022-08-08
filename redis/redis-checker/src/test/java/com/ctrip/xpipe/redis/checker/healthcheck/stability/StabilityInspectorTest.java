package com.ctrip.xpipe.redis.checker.healthcheck.stability;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2022/8/5
 */
@RunWith(MockitoJUnitRunner.class)
public class StabilityInspectorTest extends AbstractTest {

    @Mock
    private DefaultDelayPingActionCollector collector;

    @Mock
    private MetaCache metaCache;

    @Mock
    private CheckerConfig config;

    private StabilityInspector inspector;

    @Before
    public void setupStabilityInspectorTest() {
        inspector = new StabilityInspector(collector, metaCache, config);

        when(metaCache.getDc(any())).thenReturn(FoundationService.DEFAULT.getDataCenter());
        when(config.getStableRecoverAfterRounds()).thenReturn(2);
        when(config.getStableLossAfterRounds()).thenReturn(2);
        when(config.getStableResetAfterRounds()).thenReturn(2);
        when(config.getSiteStableThreshold()).thenReturn(0.8f);
        when(config.getSiteUnstableThreshold()).thenReturn(0.8f);
    }

    @Test
    public void testBecomeUnstable() {
        inspector.setStable(true);
        when(collector.getAllCachedState()).thenReturn(mockStates(HEALTH_STATE.DOWN, HEALTH_STATE.DOWN, HEALTH_STATE.DOWN, HEALTH_STATE.DOWN));

        inspector.inspect();
        Assert.assertTrue(inspector.isSiteStable());
        inspector.inspect();
        Assert.assertFalse(inspector.isSiteStable());
    }

    @Test
    public void testBecomeStable() {
        inspector.setStable(false);
        when(collector.getAllCachedState()).thenReturn(mockStates(HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY));

        inspector.inspect();
        Assert.assertFalse(inspector.isSiteStable());
        inspector.inspect();
        Assert.assertTrue(inspector.isSiteStable());
    }

    @Test
    public void testHalfDown() {
        inspector.setStable(true);
        when(collector.getAllCachedState()).thenReturn(mockStates(HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY, HEALTH_STATE.DOWN, HEALTH_STATE.DOWN));

        IntStream.range(0, 100).forEach(i -> inspector.inspect());
        Assert.assertTrue(inspector.isSiteStable());
    }

    @Test
    public void testHealthStateChangeTimeToTime() {
        inspector.setStable(true);
        IntStream.range(0, 100).forEach(i -> {
            if (0 == i % 2) {
                when(collector.getAllCachedState()).thenReturn(mockStates(HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY, HEALTH_STATE.HEALTHY));
            } else {
                when(collector.getAllCachedState()).thenReturn(mockStates(HEALTH_STATE.DOWN, HEALTH_STATE.DOWN, HEALTH_STATE.DOWN, HEALTH_STATE.DOWN));
            }
            inspector.inspect();
        });

        Assert.assertTrue(inspector.isSiteStable());
    }

    @Test
    public void testNoInterestedAndReset() {
        inspector.setStable(false);
        when(collector.getAllCachedState()).thenReturn(Collections.emptyMap());

        inspector.inspect();
        Assert.assertFalse(inspector.isSiteStable());
        inspector.inspect();
        Assert.assertTrue(inspector.isSiteStable());
    }

    private Map<HostPort, HEALTH_STATE> mockStates(HEALTH_STATE s1, HEALTH_STATE s2, HEALTH_STATE s3, HEALTH_STATE s4) {
        return new HashMap<HostPort, HEALTH_STATE>() {{
            put(HostPort.fromString("10.0.0.1:6379"), s1);
            put(HostPort.fromString("10.0.0.1:6380"), s2);
            put(HostPort.fromString("10.0.0.1:6381"), s3);
            put(HostPort.fromString("10.0.0.1:6382"), s4);
        }};
    }

}
