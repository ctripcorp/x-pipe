package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.proxy.echoserver.AdvancedEchoClient;
import com.ctrip.xpipe.redis.proxy.echoserver.EchoServer;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnelManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import io.netty.util.ResourceLeakDetector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jul 09, 2018
 */
@SpringBootApplication
public class OutBoundBufferWaterMarkTest extends AbstractProxyIntegrationTest {

    private static final int FIRST_PROXY_TCP_PORT = 8992, SEC_PROXY_TCP_PORT = 8993;

    private static final int FIRST_PROXY_TLS_PORT = 1443, SEC_PROXY_TLS_PORT = 2443;

    private static final int ECHO_SERVER_PORT = randomPort();

    @Before
    public void beforeAppTest(){
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
    }

    @Test
    public void startTwoProxyWithClientServer() throws Exception {
        TunnelManager tunnelManager = new DefaultTunnelManager().setConfig(new TestProxyConfig());
        startProxy(tunnelManager);
        startSecondaryProxy();
        startEchoServerForProxy();
        int speed = 1024;
        String protocol = String.format("+PROXY ROUTE PROXYTLS://127.0.0.1:%d TCP://127.0.0.1:%d",
                SEC_PROXY_TLS_PORT, ECHO_SERVER_PORT);
        logger.info("[wait for proxy warm up]...");
        Thread.sleep(1000);
        startEchoClient(protocol, speed);
        List<Tunnel> tunnelList = tunnelManager.tunnels();
        logger.info("[tunnels]size: {}", tunnelList.size());



        while(!Thread.interrupted()) {
            if(!tunnelList.isEmpty()) {
                Tunnel tunnel = tunnelList.get(0);
                long frontBufferSize = tunnel.frontend().getChannel().unsafe().outboundBuffer().totalPendingWriteBytes();
                long backBufferSize = tunnel.backend().getChannel().unsafe().outboundBuffer().totalPendingWriteBytes();
                Assert.assertTrue(frontBufferSize <= DefaultProxyServer.WRITE_HIGH_WATER_MARK);
                Assert.assertTrue(backBufferSize <= DefaultProxyServer.WRITE_HIGH_WATER_MARK);
                Thread.sleep(100);
            }
        }
    }

    private DefaultProxyServer startProxy(TunnelManager tunnelManager) throws Exception {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTcpPort(FIRST_PROXY_TCP_PORT).setFrontendTlsPort(FIRST_PROXY_TLS_PORT));
        server.setTunnelManager(tunnelManager);
        prepare(server);
        server.start();
        return server;
    }

    private void startEchoServerForProxy() throws Exception {
        logger.info("[startEchoServerForProxy] echo server port: {}", ECHO_SERVER_PORT);
        new Thread(new AbstractExceptionLogTask() {
            @Override
            public void doRun() throws InterruptedException {
                new EchoServer().start(ECHO_SERVER_PORT);
            }
        }).start();
    }

    private void startEchoClient(String protocol, int speed) {
        String host = "127.0.0.1";

        AdvancedEchoClient client = new AdvancedEchoClient(host, FIRST_PROXY_TCP_PORT, protocol, speed);

        try {
            client.startServer().sync();
        } catch (InterruptedException e) {
            logger.error("[doConnect]", e);
        }
    }

    protected void startSecondaryProxy() throws Exception {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTcpPort(SEC_PROXY_TCP_PORT).setFrontendTlsPort(SEC_PROXY_TLS_PORT));
        prepare(server);
        server.start();
    }
}
