package com.ctrip.framework.xpipe.redis.proxy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author limingdong
 * @create 2021/4/28
 */
public class DefaultProxyConnectProtocolTest {

    private static String PROTOCOL = "PROXY ROUTE PROXYTCP://127.0.0.1:80,PROXYTCP://127.0.0.2:80 PROXYTLS://127.0.0.0:443 TCP://127.0.0.3:8383";

    private static String OUTPUT = "+PROXY ROUTE PROXYTLS://127.0.0.0:443 TCP://127.0.0.3:8383;\r\n";

    private DefaultProxyConnectProtocol proxyConnectProtocol;

    @Before
    public void setUp() {
        proxyConnectProtocol = new DefaultProxyConnectProtocol(PROTOCOL);
    }

    @Test
    public void output() {
        byte[] output = proxyConnectProtocol.output();
        Assert.assertArrayEquals(OUTPUT.getBytes(), output);
    }
}