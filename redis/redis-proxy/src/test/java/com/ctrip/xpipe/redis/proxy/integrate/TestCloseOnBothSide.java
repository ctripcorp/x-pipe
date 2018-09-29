package com.ctrip.xpipe.redis.proxy.integrate;

import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosed;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.spy;

/**
 * @author chen.zhu
 * <p>
 * Jul 11, 2018
 */
public class TestCloseOnBothSide extends AbstractProxyIntegrationTest {

    private DefaultProxyServer proxy1, proxy2;

    private static final String PROXY_HOST = "127.0.0.1";

    private static final int PROXY_PORT1 = randomPort(), PROXY_PORT2 = randomPort();

    @Before
    public void beforeTestCloseOnBothSide() throws Exception {
        MockitoAnnotations.initMocks(this);
        proxy1 = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(-1).setFrontendTcpPort(PROXY_PORT1));
        prepare(proxy1);
        proxy1 = spy(proxy1);
        proxy1.start();

        proxy2 = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(PROXY_PORT2).setFrontendTcpPort(randomPort()));
        prepareTLS(proxy2);
        proxy2 = spy(proxy2);
        proxy2.start();
    }

    @After
    public void afterTestCloseOnBothSide() throws Exception {
        proxy1.stop();
        proxy2.stop();
    }

    @Test
    public void testCloseFromFrontendOfProxy1() throws Exception {
        Server backendServer = buildChain();

        Tunnel proxy1Tunnel = proxy1.getTunnelManager().tunnels().get(0);
        Tunnel proxy2Tunnel = proxy2.getTunnelManager().tunnels().get(0);

        Channel proxy1FrontChannel = proxy1Tunnel.frontend().getChannel();
        Channel proxy1BackendChannel = proxy1Tunnel.backend().getChannel();

        Channel proxy2FrontChannel = proxy2Tunnel.frontend().getChannel();
        Channel proxy2BackendChannel = proxy2Tunnel.backend().getChannel();

        proxy1FrontChannel.close();

        waitConditionUntilTimeOut(()->proxy1Tunnel.getState().equals(new TunnelClosed((DefaultTunnel) proxy1Tunnel)) &&
                proxy2Tunnel.getState().equals(new TunnelClosed((DefaultTunnel) proxy1Tunnel)), 1500);

        Assert.assertFalse(proxy1BackendChannel.isActive());
        Assert.assertFalse(proxy2BackendChannel.isActive());
        Assert.assertFalse(proxy2FrontChannel.isActive());

        backendServer.stop();
    }

    @Test
    public void testCloseFromBackendOfProxy2() throws Exception {

        Server backendServer = buildChain();

        Assert.assertNotNull(proxy1.getTunnelManager().tunnels());
        Assert.assertNotNull(proxy2.getTunnelManager().tunnels());

        Assert.assertFalse(proxy1.getTunnelManager().tunnels().isEmpty());
        Assert.assertFalse(proxy2.getTunnelManager().tunnels().isEmpty());

        Tunnel proxy1Tunnel = proxy1.getTunnelManager().tunnels().get(0);
        Tunnel proxy2Tunnel = proxy2.getTunnelManager().tunnels().get(0);

        Channel proxy1FrontChannel = proxy1Tunnel.frontend().getChannel();
        Channel proxy1BackendChannel = proxy1Tunnel.backend().getChannel();

        Channel proxy2FrontChannel = proxy2Tunnel.frontend().getChannel();
        Channel proxy2BackendChannel = proxy2Tunnel.backend().getChannel();

        proxy2BackendChannel.close();

        waitConditionUntilTimeOut(()->proxy1Tunnel.getState().equals(new TunnelClosed((DefaultTunnel) proxy1Tunnel)) &&
                proxy2Tunnel.getState().equals(new TunnelClosed((DefaultTunnel) proxy1Tunnel)), 1500);

        Assert.assertFalse(proxy1BackendChannel.isActive());
        Assert.assertFalse(proxy1FrontChannel.isActive());
        Assert.assertFalse(proxy2FrontChannel.isActive());

        backendServer.stop();
    }

    @Test
    public void testCloseWhenConnectAtBackend() throws InterruptedException, TimeoutException {
        int port = randomPort();
        Channel channel = clientBootstrap().connect(PROXY_HOST, PROXY_PORT1).sync().channel();

        Assert.assertTrue(channel.isActive());
        logger.info("{}", getProxyProtocol(port));
        channel.writeAndFlush(UnpooledByteBufAllocator.DEFAULT.buffer()
                .writeBytes(getProxyProtocol(port).getBytes()));

        waitConditionUntilTimeOut(()->!channel.isActive(), 2000);
    }

    private Server buildChain() throws Exception {
        Server server = startEmptyServer();
        Channel channel = clientBootstrap().connect(PROXY_HOST, PROXY_PORT1).sync().channel();
        logger.info("{}", getProxyProtocol(server.getPort()));
        channel.writeAndFlush(UnpooledByteBufAllocator.DEFAULT.buffer()
                .writeBytes(getProxyProtocol(server.getPort()).getBytes()));
        Thread.sleep(1000);
        waitConditionUntilTimeOut(()->chainEstablished(), 2000);
        return server;
    }

    private boolean chainEstablished() {
        return proxy1.getTunnelManager().tunnels() != null && proxy2.getTunnelManager().tunnels() != null
                && !proxy1.getTunnelManager().tunnels().isEmpty() && !proxy2.getTunnelManager().tunnels().isEmpty();
    }

    private String getProxyProtocol(int backendServerPort) {
        return String.format("+PROXY ROUTE PROXYTLS://127.0.0.1:%d TCP://127.0.0.1:%d\r\n", PROXY_PORT2, backendServerPort);
    }
}
