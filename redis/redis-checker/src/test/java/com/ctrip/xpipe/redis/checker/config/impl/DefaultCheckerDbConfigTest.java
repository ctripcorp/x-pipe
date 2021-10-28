package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Locale;

import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/4/7
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultCheckerDbConfigTest extends AbstractCheckerTest {

    @Mock
    private PersistenceCache persistenceCache;

    @Mock
    private CheckerConfig config;

    private DefaultCheckerDbConfig checkerDbConfig;

    @Before
    public void setupDefaultCheckerDbConfigTest() {
        when(persistenceCache.sentinelCheckWhiteList()).thenReturn(Collections.singleton("Cluster1".toLowerCase()));
        when(persistenceCache.clusterAlertWhiteList()).thenReturn(Collections.singleton("Cluster1".toLowerCase()));
        when(config.getConfigCacheTimeoutMilli()).thenReturn(10L);
        checkerDbConfig = new DefaultCheckerDbConfig(persistenceCache);
    }

    @Test
    public void testSentinelWhiteListCaseIgnore() {
        Assert.assertFalse(checkerDbConfig.shouldSentinelCheck("cLuster1"));
        Assert.assertTrue(checkerDbConfig.shouldSentinelCheck("Cluster2"));
    }

    @Test
    public void testClusterAlertWhiteList() {
        Assert.assertFalse(checkerDbConfig.shouldClusterAlert("cLuster1"));
        Assert.assertTrue(checkerDbConfig.shouldClusterAlert("Cluster2"));
        Assert.assertEquals(Collections.singleton("cluster1"), checkerDbConfig.clusterAlertWhiteList());
    }

}
