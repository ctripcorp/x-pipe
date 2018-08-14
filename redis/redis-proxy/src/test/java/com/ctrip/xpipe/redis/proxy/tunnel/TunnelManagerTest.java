package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class TunnelManagerTest extends AbstractRedisProxyServerTest {

    private TunnelManager manager;

    @Before
    public void beforeTunnelManagerTest() {
        manager = tunnelManager();
    }

    @Test
    public void testAddOrCreate() throws Exception {
        Channel frontChannel = frontChannel();
        Tunnel tunnel1 = manager.create(frontChannel, protocol());
        Tunnel tunnel2 = manager.create(frontChannel, protocol());

        Assert.assertTrue(tunnel1 == tunnel2);
    }

    @Test
    public void testRemove() throws Exception {
        Channel frontChannel = frontChannel();
        Tunnel tunnel1 = manager.create(frontChannel, protocol());
        Assert.assertNotNull(tunnel1);

        manager.remove(frontChannel);
        Assert.assertFalse(tunnel1 == manager.create(frontChannel, protocol()));
    }

    @Test
    public void testKey() {
        String host1 = "10.1.1.1", host2 = "10.2.2.2";
        int port1 = 6379, port2 = 6389;
        ProxyEndpoint endpoint1 = new DefaultProxyEndpoint(new InetSocketAddress(host1, port1));
        ProxyEndpoint endpoint2 = new DefaultProxyEndpoint(host1, port1);
        ProxyEndpoint endpoint3 = new DefaultProxyEndpoint(String.format("proxy://%s:%d", host1, port1));

        Assert.assertEquals(endpoint1, endpoint2);
        Assert.assertEquals(endpoint2, endpoint3);

        ProxyEndpoint endpoint4 = new DefaultProxyEndpoint(host2, port2);

        Assert.assertNotEquals(endpoint1, endpoint4);

        ProxyEndpoint endpoint5 = new DefaultProxyEndpoint(String.format("proxytls://%s:%d", host1, port1));
        Assert.assertNotEquals(endpoint3, endpoint5);
    }
}