package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/4/7
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCheckerDbConfigTest extends AbstractCheckerTest {

    @Mock
    private Persistence persistence;

    @Mock
    private CheckerConfig config;

    private DefaultCheckerDbConfig checkerDbConfig;

    @Before
    public void setupDefaultCheckerDbConfigTest() {
        when(persistence.sentinelCheckWhiteList()).thenReturn(Collections.singleton("Cluster1"));
        when(persistence.clusterAlertWhiteList()).thenReturn(Collections.singleton("Cluster1"));
        when(config.getConfigCacheTimeoutMilli()).thenReturn(10L);
        checkerDbConfig = new DefaultCheckerDbConfig(persistence, config);
    }

    @Test
    public void testSentinelWhiteListCaseIgnore() {
        Assert.assertFalse(checkerDbConfig.shouldSentinelCheck("cLuster1"));
        Assert.assertTrue(checkerDbConfig.shouldSentinelCheck("Cluster2"));
    }

    @Test
    public void testSentinelWhiteListConfigCacheExpire() {
        Assert.assertFalse(checkerDbConfig.shouldSentinelCheck("cLuster1"));
        when(persistence.sentinelCheckWhiteList()).thenReturn(Collections.emptySet());
        Assert.assertFalse(checkerDbConfig.shouldSentinelCheck("cLuster1"));
        sleep(10);
        Assert.assertTrue(checkerDbConfig.shouldSentinelCheck("cLuster1"));
    }

    @Test
    public void testClusterAlertWhiteList() {
        Assert.assertFalse(checkerDbConfig.shouldClusterAlert("cLuster1"));
        Assert.assertTrue(checkerDbConfig.shouldClusterAlert("Cluster2"));
        Assert.assertEquals(Collections.singleton("cluster1"), checkerDbConfig.clusterAlertWhiteList());
        when(persistence.clusterAlertWhiteList()).thenReturn(Collections.emptySet());
        sleep(10);
        Assert.assertTrue(checkerDbConfig.shouldClusterAlert("cLuster1"));
        Assert.assertEquals(Collections.emptySet(), checkerDbConfig.clusterAlertWhiteList());
    }

}
