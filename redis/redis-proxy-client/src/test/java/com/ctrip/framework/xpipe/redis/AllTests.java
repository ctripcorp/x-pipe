package com.ctrip.framework.xpipe.redis;

import com.ctrip.framework.xpipe.redis.agent.adaptor.SocketAdaptorTest;
import com.ctrip.framework.xpipe.redis.agent.adaptor.SocketChannelImplAdaptorTest;
import com.ctrip.framework.xpipe.redis.utils.ConnectionUtilTest;
import com.ctrip.framework.xpipe.redis.utils.ProxyUtilTest;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.net.InetAddress;


@RunWith(Suite.class)
@Suite.SuiteClasses(value = {

        ProxyUtilTest.class,
        ConnectionUtilTest.class,
        SocketAdaptorTest.class,
        SocketChannelImplAdaptorTest.class,
        ProxyRegistryTest.class

})

public class AllTests {

    public static final String IP = "127.0.0.1";

    public static final int PORT = 1443;

    private static MockWebServer webServer;

    @BeforeClass
    public static void setUp() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName(IP), PORT);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        webServer.close();
    }
}
