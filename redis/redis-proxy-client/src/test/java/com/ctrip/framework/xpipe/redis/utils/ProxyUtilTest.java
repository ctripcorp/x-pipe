package com.ctrip.framework.xpipe.redis.utils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Socket;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;

/**
 * @Author limingdong
 * @create 2021/4/22
 */
public class ProxyUtilTest extends AbstractProxyTest {

    private String EXPECT_PROTOCOL = String.format("+PROXY ROUTE PROXYTLS://127.0.0.0:443 TCP://%s:%s;\r\n", IP, PORT);

    private ProxyUtil proxyUtil = ProxyUtil.getInstance();

    @Before
    public void setUp() {
        socket = new Socket();
        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
    }

    @After
    public void tearDown() throws Exception {
        proxyUtil.removeProxyAddress(socket);
        proxyUtil.unregisterProxy(IP, PORT);
    }

    @Test
    public void testProxyConnectProtocol() {
        Assert.assertFalse(proxyUtil.needProxy(new InetSocketAddress(IP, PORT + 1)));
        Assert.assertTrue(proxyUtil.needProxy(socketAddress));

        InetSocketAddress inetSocketAddress = proxyUtil.getProxyAddress(socket, socketAddress);
        Assert.assertNotNull(inetSocketAddress);
        Assert.assertTrue(inetSocketAddress.equals(new InetSocketAddress(PROXY_IP_1, PROXY_PORT)) || inetSocketAddress.equals(new InetSocketAddress(PROXY_IP_2, PROXY_PORT)));

        byte[] protocolBytes = proxyUtil.getProxyConnectProtocol(socket);
        String protocol = new String(protocolBytes);
        Assert.assertEquals(EXPECT_PROTOCOL, protocol);
    }
}