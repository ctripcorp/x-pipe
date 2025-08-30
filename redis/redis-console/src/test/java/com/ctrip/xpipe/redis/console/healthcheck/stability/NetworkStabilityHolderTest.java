package com.ctrip.xpipe.redis.console.healthcheck.stability;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.exception.NotEnoughResultsException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class NetworkStabilityHolderTest {

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleConfig config;

    @Mock
    private NetworkStabilityInspector inspector;

    @InjectMocks
    private ConsoleNetworkStabilityHolder networkStabilityHolder;

    private static final String JQ = "jq";
    private static final String OY = "oy";
    private static final String ALI = "ali";
    private static final HostPort server1 = new HostPort("127.0.0.1", 8080);
    private static final HostPort server2 = new HostPort("127.0.0.2", 8080);
    private static final HostPort server3 = new HostPort("127.0.0.3", 8080);

    @Before
    public void beforeNetworkStabilityHolderTest() throws Exception {
        when(config.getQuorum()).thenReturn(2);
    }

    @Test
    public void testDelegateDc() {
        when(config.checkDcNetwork()).thenReturn(false);
        networkStabilityHolder.check();
        Assert.assertFalse(networkStabilityHolder.isolated());
        verify(metaCache, never()).regionDcs(JQ);

        when(config.checkDcNetwork()).thenReturn(true);
        when(metaCache.regionDcs(JQ)).thenReturn(Lists.newArrayList(JQ, OY));
        when(config.delegateDc()).thenReturn("");


        networkStabilityHolder.check();
        Assert.assertFalse(networkStabilityHolder.isolated());
        verify(metaCache, times(1)).regionDcs(JQ);
        verify(config, times(1)).delegateDc();
        verify(consoleServiceManager, never()).getDcIsolatedCheckResult(anyString());

        when(config.delegateDc()).thenReturn(OY);
        when(consoleServiceManager.getDcIsolatedCheckResult(OY)).thenReturn(true);
        networkStabilityHolder.check();
        Assert.assertTrue(networkStabilityHolder.isolated());

        when(consoleServiceManager.getDcIsolatedCheckResult(OY)).thenReturn(null);
        networkStabilityHolder.check();
        Assert.assertTrue(networkStabilityHolder.isolated());

        when(consoleServiceManager.getDcIsolatedCheckResult(OY)).thenReturn(false);
        networkStabilityHolder.check();
        Assert.assertFalse(networkStabilityHolder.isolated());
    }


    @Test
    public void testAggregate() throws Exception {

        when(config.checkDcNetwork()).thenReturn(true);
        when(metaCache.regionDcs(JQ)).thenReturn(Lists.newArrayList(JQ, OY, ALI));

        when(inspector.isolated()).thenReturn(false);
        networkStabilityHolder.check();
        Assert.assertFalse(networkStabilityHolder.isolated());
        verify(metaCache, times(1)).regionDcs(JQ);
        verify(config, never()).delegateDc();
        verify(consoleServiceManager, never()).getDcIsolatedCheckResult(anyString());
        verify(consoleServiceManager, never()).getAllDcIsolatedCheckResult();

        when(inspector.isolated()).thenReturn(true);
        Map<String, Boolean> allServerResults = Maps.newHashMap();
        allServerResults.put(server1.toString(), true);
        allServerResults.put(server2.toString(), false);
        allServerResults.put(server3.toString(), true);
        when(consoleServiceManager.getAllDcIsolatedCheckResult()).thenReturn(allServerResults);
        networkStabilityHolder.check();
        Assert.assertFalse(networkStabilityHolder.isolated());

        allServerResults.put(server2.toString(), true);
        when(consoleServiceManager.getAllDcIsolatedCheckResult()).thenReturn(allServerResults);
        networkStabilityHolder.check();
        Assert.assertTrue(networkStabilityHolder.isolated());

        doThrow(new NotEnoughResultsException("test")).when(consoleServiceManager).getAllDcIsolatedCheckResult();
        networkStabilityHolder.check();
        Assert.assertTrue(networkStabilityHolder.isolated());

        when(inspector.isolated()).thenReturn(false);
        networkStabilityHolder.check();
        Assert.assertFalse(networkStabilityHolder.isolated());
    }

}
