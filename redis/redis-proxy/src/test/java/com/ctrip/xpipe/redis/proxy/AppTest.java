package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.proxy.echoserver.AdvancedEchoClient;
import com.ctrip.xpipe.redis.proxy.echoserver.EchoServer;
import com.ctrip.xpipe.redis.proxy.integrate.AbstractProxyIntegrationTest;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.dianping.cat.Cat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
@SpringBootApplication
public class AppTest extends AbstractProxyIntegrationTest {

    private static Logger logger = LoggerFactory.getLogger(AppTest.class);

    private ScheduledExecutorService scheduled;

    private static final int FIRST_PROXY_TCP_PORT = 8992, SEC_PROXY_TCP_PORT = 8993;

    private static final int FIRST_PROXY_TLS_PORT = 1443, SEC_PROXY_TLS_PORT = 2443;

    private static final int ECHO_SERVER_PORT = randomPort();

    @Before
    public void beforeAppTest(){
        System.setProperty(AbstractProfile.PROFILE_KEY, AbstractProfile.PROFILE_NAME_TEST);
    }

    @Test
    public void startTwoProxyWithClientServer() throws Exception {
        startFirstProxy();
        startSecondaryProxy();
        startEchoServerForProxy();
        int speed = 5 * 1024 * 1024;
        String protocol = String.format("+PROXY ROUTE PROXYTLS://127.0.0.1:%d TCP://127.0.0.1:%d",
                SEC_PROXY_TLS_PORT, ECHO_SERVER_PORT);
        logger.info("[wait for proxy warm up]...");
        Thread.sleep(1000);
        startEchoClient(FIRST_PROXY_TCP_PORT, protocol, speed);
    }

    @Test
    public void startOneProxyWithClientServer() throws Exception {
        startFirstProxy();
        startEchoServerForProxy();
        int speed = 5 * 1024 * 1024;
        String protocol = String.format("+PROXY ROUTE TCP://127.0.0.1:%d", ECHO_SERVER_PORT);
        logger.info("[wait for proxy warm up]...");
        Thread.sleep(1000);
        startEchoClient(FIRST_PROXY_TCP_PORT, protocol, speed);
    }

    private void startFirstProxy() throws Exception {
        // uncomment disable netty bytebuf test
//        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.DISABLED);
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTcpPort(FIRST_PROXY_TCP_PORT).setFrontendTlsPort(FIRST_PROXY_TLS_PORT));
        prepare(server);
        server.start();
    }


    private void startSecondaryProxy() throws Exception {
        DefaultProxyServer server = new DefaultProxyServer().setConfig(new TestProxyConfig()
                .setFrontendTcpPort(SEC_PROXY_TCP_PORT).setFrontendTlsPort(SEC_PROXY_TLS_PORT));
        prepare(server);
        server.start();
    }

    private void startEchoClient(int firstProxyPort, String protocol, int speed) throws Exception {
        String host = "127.0.0.1";
        logger.info("[startEchoClient] ");
        doConnect(host, firstProxyPort, protocol, speed);
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

    private void doConnect(String host, int port, String protocol, int speed) {
        AdvancedEchoClient client = new AdvancedEchoClient(host, port, protocol, speed);

        Cat.logEvent("Start", System.nanoTime()+"");
        try {
            client.startServer().channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("[doConnect]", e);
        } finally {
            scheduled.schedule(()->doConnect(host, port, protocol, speed), 1, TimeUnit.MINUTES);
        }

    }
}
