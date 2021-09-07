package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;
import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/4/22
 */
public class ConnectionUtilTest extends AbstractProxyTest {

    @Before
    public void setUp() throws IOException {
        super.setUp();
    }

    @Test
    public void testConnectProxyThroughSocket() throws Exception {
        SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertEquals(sa, socketAddress);

        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertNotEquals(sa, socketAddress);
        Assert.assertTrue(sa.equals(new InetSocketAddress(PROXY_IP_1, PROXY_PORT)) || sa.equals(new InetSocketAddress(PROXY_IP_2, PROXY_PORT)));
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) socketAddress, 500);  // suppose socketAddress is proxy
        } catch (Throwable t) {
            Assert.fail("[Connect] to proxy failed");
        } finally {
            ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        }
    }

    @Test
    public void testConnectProxyThroughSocketChannel() throws Exception {
        SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertEquals(sa, socketAddress);

        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);

        SocketChannel socketChannel = mock(SocketChannel.class);
        try {

            SocketAddress proxyAddress = ConnectionUtil.getAddress(socketChannel, socketAddress);
            boolean connected = ConnectionUtil.connectToProxy(socketChannel, proxyAddress);  // suppose socketAddress is proxy
            Assert.assertFalse(connected);
            ConnectionUtil.sendProtocolToProxy(socketChannel);
            verify(socketChannel, times(1)).write(any(ByteBuffer.class));
        } catch (Throwable t) {
            Assert.fail("[Connect] to proxy failed");
        } finally {
            ProxyUtil.getInstance().unregisterProxy(IP, PORT);
            ConnectionUtil.removeAddress(socketChannel);
        }
    }
    
    

}