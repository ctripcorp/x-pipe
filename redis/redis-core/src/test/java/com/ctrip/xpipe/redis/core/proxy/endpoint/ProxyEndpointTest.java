package com.ctrip.xpipe.redis.core.proxy.endpoint;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

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
    public void testNotReverseDNS() {
        long startTime = System.currentTimeMillis();
        ProxyEndpoint endpoint1 = new DefaultProxyEndpoint(new InetSocketAddress("100.100.100.100", 8080));
        long spendTime = System.currentTimeMillis() - startTime;
        Assert.assertEquals(true, spendTime < 2000);
    }

    @Test
    public void testRawUri() {
    }
}