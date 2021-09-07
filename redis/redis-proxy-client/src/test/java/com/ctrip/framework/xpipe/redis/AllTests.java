package com.ctrip.framework.xpipe.redis;

import com.ctrip.framework.xpipe.redis.instrument.adapter.SocketAdapterTest;
import com.ctrip.framework.xpipe.redis.instrument.adapter.SocketChannelImplAdapterTest;
import com.ctrip.framework.xpipe.redis.proxy.DefaultProxyConnectProtocolTest;
import com.ctrip.framework.xpipe.redis.proxy.RouteOptionParserTest;
import com.ctrip.framework.xpipe.redis.utils.*;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.net.InetAddress;


@RunWith(Suite.class)
@Suite.SuiteClasses(value = {

        DefaultProxyConnectProtocolTest.class,
        RouteOptionParserTest.class,
        ProxyUtilTest.class,
        ConnectionUtilTest.class,
        SocketAdapterTest.class,
        SocketChannelImplAdapterTest.class,
        JarFileUrlJarTest.class,
        JdkVersionTest.class,
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
