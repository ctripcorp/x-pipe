package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class ProxyEndpointTest {

    @Test
    public void testIsSslEnabled() {
        ProxyEndpoint endpoint1 = new DefaultProxyEndpoint("proxy://127.0.0.1:6379");
        ProxyEndpoint endpoint2 = new DefaultProxyEndpoint("proxytls://127.0.0.1:6379");

        Assert.assertFalse(endpoint1.isSslEnabled());
        Assert.assertTrue(endpoint2.isSslEnabled());
    }

    @Test
    public void testRawUri() {
    }
}