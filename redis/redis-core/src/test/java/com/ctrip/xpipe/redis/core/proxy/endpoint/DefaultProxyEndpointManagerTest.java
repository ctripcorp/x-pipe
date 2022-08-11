package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jul 11, 2018
 */
public class DefaultProxyEndpointManagerTest {

    @Test
    public void testFirst2TimesNotHealthy3rdTimeOk() throws InterruptedException {
        DefaultProxyEndpointManager manager = new DefaultProxyEndpointManager(()-> 1);
        ProxyEndpointHealthChecker checker = mock(ProxyEndpointHealthChecker.class);
        when(checker.checkConnectivity(any(ProxyEndpoint.class))).thenReturn(false).thenReturn(false).thenReturn(true);
        manager.setHealthChecker(checker);
        manager.storeProxyEndpoints(Lists.newArrayList(new DefaultProxyEndpoint("RPOXYTCP://127.0.0.1:9090")));
        Thread.sleep(4000);
        Assert.assertFalse(manager.getAvailableProxyEndpoints().isEmpty());
    }
}