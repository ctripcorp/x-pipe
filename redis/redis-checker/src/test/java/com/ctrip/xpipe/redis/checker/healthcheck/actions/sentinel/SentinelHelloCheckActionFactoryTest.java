package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerIntegrationTest;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.SentinelCollector4Keeper;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloCheckActionFactoryTest extends AbstractCheckerIntegrationTest {

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

        CheckerDbConfig config = mock(CheckerDbConfig.class);
        when(config.isSentinelAutoProcess()).thenReturn(true);
        factory.setCheckerDbConfig(config);
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