package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.core.protocal.protocal.SimpleStringParser;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class ProxyProtocolHandlerTest extends AbstractRedisProxyServerTest {

    @Test
    public void testIsProxyProtocol() {
        Assert.assertTrue(new ProxyProtocolHandler().isProxyProtocol(protocol().output()));
        Assert.assertFalse(new ProxyProtocolHandler().isProxyProtocol(new SimpleStringParser("OK").format()));
    }
}