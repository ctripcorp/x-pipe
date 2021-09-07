package com.ctrip.framework.xpipe.redis.utils;

import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;

/**
 * @Author limingdong
 * @create 2021/4/22
 */
public class AbstractProxyTest {

    protected static final String PROXY_IP_1 = "127.0.0.1";

    protected static final String PROXY_IP_2 = "127.0.0.2";

    protected static final int PROXY_PORT = 80;

    protected String ROUTE_INFO = String.format("PROXY ROUTE PROXYTCP://%s:%s,PROXYTCP://%s:%s PROXYTLS://127.0.0.0:443 TCP", PROXY_IP_1, PROXY_PORT, PROXY_IP_2, PROXY_PORT);

    protected SocketAddress socketAddress = new InetSocketAddress(IP, PORT);

    protected Socket socket;

    @Before
    public void setUp() throws IOException, InterruptedException {
        socket = new Socket();
    }

}
