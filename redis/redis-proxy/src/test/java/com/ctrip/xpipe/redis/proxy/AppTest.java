package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.spring.AbstractProfile;
import io.netty.util.ResourceLeakDetector;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
@SpringBootApplication
public class AppTest extends AbstractProxyIntegrationTest {

    @Before
    public void beforeAppTest(){
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
    }

    @Test
    public void start8992() throws Exception {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        System.setProperty("server.port", "9992");
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTcpPort(8992).setFrontendTlsPort(1443));
        prepare(server);
        server.start();
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void start8993() throws Exception {
        System.setProperty("server.port", "9993");
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig().setFrontendTcpPort(8993).setFrontendTlsPort(2443));
        prepare(server);
        server.start();
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void startListenServer() throws Exception {
        startListenServer(8009).sync().channel().closeFuture().sync();
    }
}
