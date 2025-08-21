package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.DefaultChannelPromise;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.net.InetSocketAddress;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 *         <p>
 *         May 15, 2018
 */
public class DefaultTunnelManagerTest extends AbstractRedisProxyServerTest {

    @Mock
    private DefaultTunnelManager manager;

    @Mock
    private Tunnel tunnel;

    @Before
    public void beforeDefaultTunnelManagerTest() {
        manager = (DefaultTunnelManager) tunnelManager();
        manager = spy(manager);
    }

    @Test
    public void testDoClean() throws Exception {
        startListenServer(8009);
        Channel frontChannel1 = fakeChannel();
        Channel frontChannel2 = fakeChannel();
        manager.addTunnel(frontChannel1, tunnel);
        manager.addTunnel(frontChannel2, tunnel);
        Assert.assertEquals(2, manager.tunnels().size());

        when(frontChannel1.isActive()).thenReturn(false);
        when(frontChannel2.isActive()).thenReturn(true);
        manager.doClean();
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