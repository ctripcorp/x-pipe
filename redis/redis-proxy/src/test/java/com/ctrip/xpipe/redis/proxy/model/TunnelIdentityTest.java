package com.ctrip.xpipe.redis.proxy.model;

import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

/**
 * @author chen.zhu
 * <p>
 * Aug 03, 2018
 */
public class TunnelIdentityTest extends AbstractProxyIntegrationTest {

    private DefaultProxyServer proxy1, proxy2;

    private static final String PROXY_HOST = "127.0.0.1";

    private static final int PROXY_PORT1 = randomPort(), PROXY_PORT2 = randomPort();

    @Before
    public void beforeTestCloseOnBothSide() throws Exception {
        MockitoAnnotations.initMocks(this);
        proxy1 = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(-1).setFrontendTcpPort(PROXY_PORT1));
        prepare(proxy1);
        proxy1.start();

        proxy2 = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTlsPort(PROXY_PORT2).setFrontendTcpPort(randomPort()));
        prepareTLS(proxy2);
        proxy2.start();
    }

    @After
    public void afterTestCloseOnBothSide() throws Exception {
        proxy1.stop();
        proxy2.stop();
    }

    @Test
    public void testBackendNotNull() throws Exception {
        buildChain();
        Tunnel tunnel = proxy1.getTunnelManager().tunnels().get(0);
        TunnelIdentity identity = tunnel.identity();
        Assert.assertNotNull(identity.getBackend());
    }

    private Server buildChain() throws Exception {
        Server server = startEmptyServer();
        Channel channel = clientBootstrap().connect(PROXY_HOST, PROXY_PORT1).sync().channel();
        logger.info("{}", getProxyProtocol(server.getPort()));
        channel.writeAndFlush(UnpooledByteBufAllocator.DEFAULT.buffer()
                .writeBytes(getProxyProtocol(server.getPort()).getBytes()));
        Thread.sleep(1000);
        waitConditionUntilTimeOut(()->chainEstablished(), 1500);
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