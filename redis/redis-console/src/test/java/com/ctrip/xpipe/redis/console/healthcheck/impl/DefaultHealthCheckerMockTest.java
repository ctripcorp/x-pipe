package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.console.healthcheck.meta.MetaChangeManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultHealthCheckerMockTest extends AbstractConsoleTest {

    @InjectMocks
    private DefaultHealthChecker checker;

    @Mock
    private MetaCache metaCache;

    @Mock
    private HealthCheckInstanceManager instanceManager;

    @Mock
    private MetaChangeManager metaChangeManager;

    @Mock
    private ConsoleConfig consoleConfig;

    @Before
    public void setupDefaultHealthCheckerMockTest() {
        Mockito.when(consoleConfig.getIgnoredHealthCheckDc()).thenReturn(Collections.emptySet());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
    }

    @Test
    public void testGenerateHealthCheckInstances() throws Exception {
        Set<HostPort> loadedRedises = new HashSet<>();
        Set<HostPort> expectedRedises = Sets.newHashSet(new HostPort("127.0.0.1", 6379),
                new HostPort("127.0.0.2", 6379),
                new HostPort("127.0.0.3", 6379),
                new HostPort("127.0.0.1", 6579),
                new HostPort("127.0.0.2", 6579));

        Mockito.doAnswer(invocation -> {
            RedisMeta redis = invocation.getArgumentAt(0, RedisMeta.class);
            HostPort redisHostPort = new HostPort(redis.getIp(), redis.getPort());
            Assert.assertTrue(expectedRedises.contains(redisHostPort));
            loadedRedises.add(redisHostPort);
            return null;
        }).when(instanceManager).getOrCreate(Mockito.any());
        checker.doInitialize();

        Assert.assertEquals(expectedRedises, loadedRedises);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "health-instance-load-test.xml";
    }

}
