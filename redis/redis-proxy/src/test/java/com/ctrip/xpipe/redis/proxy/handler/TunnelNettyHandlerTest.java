package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */
public class TunnelNettyHandlerTest extends AbstractRedisProxyServerTest {

    private TunnelNettyHandler handler;

    @Before
    public void beforeTunnelNettyHandlerTest() {
        handler = new TunnelNettyHandler(tunnelManager());
    }


    @Test
    public void testChannelRead() throws Exception {
        handler.channelRead(null, protocol());
    }
}