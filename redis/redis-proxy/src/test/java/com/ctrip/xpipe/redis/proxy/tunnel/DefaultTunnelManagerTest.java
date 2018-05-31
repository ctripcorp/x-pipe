package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        Channel frontChannel1 = fakeChannel();
        Channel frontChannel2 = fakeChannel();
        manager.create(frontChannel1, protocol("Proxy Route proxy://127.0.0.1:8009"));
        manager.create(frontChannel2, protocol("Proxy Route proxy://127.0.0.1:8009"));

        Assert.assertEquals(2, manager.tunnels().size());

        when(frontChannel1.isActive()).thenReturn(false);
        when(frontChannel2.isActive()).thenReturn(true);
        manager.doClean();

        Assert.assertEquals(1, manager.tunnels().size());
    }

    @Test
    public void testDoClean2() throws Exception {
        startListenServer(8009);
        Channel frontChannel1 = fakeChannel();
        Channel frontChannel2 = fakeChannel();
        when(frontChannel2.isActive()).thenReturn(true);
        Tunnel tunnel1 = manager.create(frontChannel1, protocol("Proxy Route proxy://127.0.0.1:8009"));
        manager.create(frontChannel2, protocol());

        Assert.assertEquals(2, manager.tunnels().size());

        tunnel1.release();
        Assert.assertEquals(1, manager.tunnels().size());
    }

    private Channel fakeChannel() {
        Channel channel = mock(Channel.class);
        ChannelConfig config = mock(ChannelConfig.class);
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", randomPort()));
        when(channel.config()).thenReturn(config);
        when(config.isAutoRead()).thenReturn(false);
        when(channel.writeAndFlush(any())).thenReturn(new DefaultChannelPromise(channel));
        return channel;
    }

    @Test
    public void update() {
    }
}