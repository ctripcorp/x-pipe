package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class DefaultTunnelManagerTest extends AbstractRedisProxyServerTest {

    private DefaultTunnelManager manager;

    @Before
    public void beforeDefaultTunnelManagerTest() {
        manager = (DefaultTunnelManager) tunnelManager();
    }

    @Test
    public void testDoClean() throws Exception {
        startListenServer(8009);
        Channel frontChannel1 = frontChannel();
        Channel frontChannel2 = frontChannel();
        manager.getOrCreate(frontChannel1, protocol("Proxy Route proxy://127.0.0.1:8009"));
        manager.getOrCreate(frontChannel2, protocol("Proxy Route proxy://127.0.0.1:8009"));

        Assert.assertEquals(2, manager.tunnels().size());

        frontChannel1.close().sync();
        manager.doClean();

        Assert.assertEquals(1, manager.tunnels().size());
    }

    @Test
    public void testDoClean2() throws Exception {
        startListenServer(8009);
        Channel frontChannel1 = frontChannel();
        Channel frontChannel2 = frontChannel();
        Tunnel tunnel1 = manager.getOrCreate(frontChannel1, protocol("Proxy Route proxy://127.0.0.1:8009"));
        manager.getOrCreate(frontChannel2, protocol());

        Assert.assertEquals(2, manager.tunnels().size());

        tunnel1.release();
        Assert.assertEquals(1, manager.tunnels().size());

        logger.info("[testDoClean2] {}", manager.tunnels());
        Assert.assertEquals(1, manager.tunnels().size());
    }

    @Test
    public void update() {
    }
}