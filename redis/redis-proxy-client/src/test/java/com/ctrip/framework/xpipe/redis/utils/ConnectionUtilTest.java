package com.ctrip.framework.xpipe.redis.utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.PollSelectorProvider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;

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
        final int[] writeCount = {0};

        SocketChannel socketChannel = new SocketChannel(new PollSelectorProvider()) {
            @Override
            protected void implCloseSelectableChannel() throws IOException {

            }

            @Override
            protected void implConfigureBlocking(boolean block) throws IOException {

            }

            @Override
            public SocketChannel bind(SocketAddress local) throws IOException {
                return null;
            }

            @Override
            public <T> SocketChannel setOption(SocketOption<T> name, T value) throws IOException {
                return null;
            }

            @Override
            public <T> T getOption(SocketOption<T> name) throws IOException {
                return null;
            }

            @Override
            public Set<SocketOption<?>> supportedOptions() {
                return null;
            }

            @Override
            public SocketChannel shutdownInput() throws IOException {
                return null;
            }

            @Override
            public SocketChannel shutdownOutput() throws IOException {
                return null;
            }

            @Override
            public Socket socket() {
                return null;
            }

            @Override
            public boolean isConnected() {
                return false;
            }

            @Override
            public boolean isConnectionPending() {
                return false;
            }

            @Override
            public boolean connect(SocketAddress remote) throws IOException {
                return false;
            }

            @Override
            public boolean finishConnect() throws IOException {
                return false;
            }

            @Override
            public SocketAddress getRemoteAddress() throws IOException {
                return null;
            }

            @Override
            public int read(ByteBuffer dst) throws IOException {
                return 0;
            }

            @Override
            public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                return 0;
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                writeCount[0]++;
                return 0;
            }

            @Override
            public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                return 0;
            }

            @Override
            public SocketAddress getLocalAddress() throws IOException {
                return null;
            }
        };

        try {

            ConnectionUtil.getAddress(socketChannel, socketAddress);
            boolean connected = ConnectionUtil.connectToProxy(socketChannel, socketAddress);  // suppose socketAddress is proxy
            Assert.assertFalse(connected);
            ConnectionUtil.sendProtocolToProxy(socketChannel);
            Assert.assertEquals(writeCount[0], 1);
        } catch (Throwable t) {
            Assert.fail("[Connect] to proxy failed");
        } finally {
            ProxyUtil.getInstance().unregisterProxy(IP, PORT);
            ConnectionUtil.removeAddress(socketChannel);
        }
    }

}