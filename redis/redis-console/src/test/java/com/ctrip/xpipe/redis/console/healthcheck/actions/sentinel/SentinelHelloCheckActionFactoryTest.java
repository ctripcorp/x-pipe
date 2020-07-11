package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.SentinelCollector4Keeper;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloCheckActionFactoryTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private SentinelHelloCheckActionFactory factory;

    @Autowired
    @Qualifier("defaultSentinelHelloCollector")
    private DefaultSentinelHelloCollector collector1;

    @Autowired
    private SentinelCollector4Keeper collector2;

    @Autowired
    private MetaCache metaCache;

    @Before
    public void beforeSentinelHelloCheckActionFactoryTest() {
        metaCache = spy(metaCache);
        ConsoleDbConfig config = mock(ConsoleDbConfig.class);
        when(config.isSentinelAutoProcess()).thenReturn(true);
        factory.setConsoleDbConfig(config);
    }

    @Test
    public void testCreate() throws Exception {
        collector1 = spy(collector1);
        collector2 = spy(collector2);
        SentinelHelloCheckAction action = (SentinelHelloCheckAction) factory
                .create(newRandomRedisHealthCheckInstance("dc2", randomPort()));
        Assert.assertTrue(action.getListeners().size() > 0);
        Assert.assertTrue(action.getControllers().size() > 0);
        logger.info("[listeners] {}", action.getListeners());
        logger.info("[controller] {}", action.getControllers());

        Assert.assertTrue(action.getListeners().stream().allMatch(listener -> listener instanceof OneWaySupport));
        Assert.assertTrue(action.getControllers().stream().allMatch(controller -> controller instanceof OneWaySupport));

        action.processSentinelHellos();
    }

    @Test
    public void testCreateForBiDirectionInstance() throws Exception {
        SentinelHelloCheckAction action = (SentinelHelloCheckAction) factory
                .create(newRandomRedisHealthCheckInstance("dc1", ClusterType.BI_DIRECTION, randomPort()));
        Assert.assertTrue(action.getListeners().size() > 0);
        Assert.assertTrue(action.getControllers().size() > 0);

        Assert.assertTrue(action.getListeners().stream().allMatch(listener -> listener instanceof BiDirectionSupport));
        Assert.assertTrue(action.getControllers().stream().allMatch(controller -> controller instanceof BiDirectionSupport));

        action.processSentinelHellos();
    }

    @Test
    public void testSupport() {
        Assert.assertEquals(SentinelHelloCheckAction.class, factory.support());
    }
}