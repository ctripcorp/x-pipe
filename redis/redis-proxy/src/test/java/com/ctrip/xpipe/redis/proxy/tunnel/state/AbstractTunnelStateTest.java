package com.ctrip.xpipe.redis.proxy.tunnel.state;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class AbstractTunnelStateTest extends AbstractRedisProxyServerTest {

    @Test
    public void isPreStateOf() throws Exception {
        Assert.assertTrue(new TunnelHalfEstablished(null).nextAfterSuccess().equals(new TunnelEstablished(null)));
        Assert.assertTrue(new TunnelHalfEstablished(null).isValidNext(new TunnelEstablished(null)));
    }
}