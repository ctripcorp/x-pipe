package com.ctrip.framework.xpipe.redis;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * @Author limingdong
 * @create 2021/4/23
 */
public class ProxyRegistryTest {

    private String EXPECT_PROTOCOL = "PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 PROXYTLS://127.0.0.0:443 TCP";

    @Test
    public void testProxyRegistry() throws Exception {
        String ip = "127.0.1.2";
        int port = new Random().nextInt(1000) + 1;
        Assert.assertNull(ProxyRegistry.unregisterProxy(ip, port));

        Assert.assertFalse(ProxyRegistry.registerProxy(ip, port, null));

        Assert.assertTrue(ProxyRegistry.registerProxy(ip, port, EXPECT_PROTOCOL));
        Assert.assertNotNull(ProxyRegistry.unregisterProxy(ip, port));
    }

    @Test
    public void testProxyRegistry2() throws Exception {
        String ip = "127.0.1.2";
        int port = new Random().nextInt(1000) + 1;
        Assert.assertNull(ProxyRegistry.unregisterProxy(ip, port));

        Assert.assertFalse(ProxyRegistry.registerProxy(ip, port, null));

        Assert.assertTrue(ProxyRegistry.registerProxy(ip, port, EXPECT_PROTOCOL + "://" + ip + ":" + port));
        Assert.assertNotNull(ProxyRegistry.unregisterProxy(ip, port));
    }

    @Test
    public void testProxyRegistryWithKey() throws Exception {
        String ip = "127.0.1.2";
        String registerKey = "registerKey";
        int port = new Random().nextInt(1000) + 1;
        Assert.assertNull(ProxyRegistry.unregisterProxy(registerKey, ip, port));

        Assert.assertFalse(ProxyRegistry.registerProxy(registerKey, ip, port, null));

        Assert.assertTrue(ProxyRegistry.registerProxy(registerKey, ip, port, EXPECT_PROTOCOL));
        Assert.assertNotNull(ProxyRegistry.unregisterProxy(registerKey, ip, port));
    }

    @Test
    public void testProxyRegistry2WithKey() throws Exception {
        String ip = "127.0.1.2";
        String registerKey = "registerKey";
        int port = new Random().nextInt(1000) + 1;
        Assert.assertNull(ProxyRegistry.unregisterProxy(registerKey, ip, port));

        Assert.assertFalse(ProxyRegistry.registerProxy(registerKey, ip, port, null));

        Assert.assertTrue(ProxyRegistry.registerProxy(registerKey, ip, port, EXPECT_PROTOCOL + "://" + ip + ":" + port));
        Assert.assertNotNull(ProxyRegistry.unregisterProxy(registerKey, ip, port));
    }

}