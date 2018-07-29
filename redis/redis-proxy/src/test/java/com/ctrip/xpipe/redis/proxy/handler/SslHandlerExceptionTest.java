package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.DefaultProxyServer;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Jul 22, 2018
 */
public class SslHandlerExceptionTest extends AbstractProxyIntegrationTest {

    @Test
    public void testThrowException() throws Exception {
        int randomPort = randomPort();
        DefaultProxyServer proxy = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTlsPort(randomPort).setFrontendTcpPort(randomPort()));
        prepareTLS(proxy);
        proxy.start();

        clientBootstrap().connect("127.0.0.1", randomPort).sync().channel();

        Thread.sleep(1000 * 60 * 60);
    }
}
