package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;
import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/4/22
 */
public class ConnectionUtilTest extends AbstractProxyTest {

    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        ProxyUtil.getInstance().setCheckInterval(100);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(true);
            }
        });
        Thread.sleep(110);
        ProxyUtil.getInstance().setChecker(null);
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
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            ConnectionUtil.connectToProxy(socket, new ProxyInetSocketAddress(inetSocketAddress.getAddress(), inetSocketAddress.getPort()), 500);  // suppose socketAddress is proxy
        } catch (Throwable t) {
            Assert.fail("[Connect] to proxy failed");
        } finally {
            ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        }
    }
    
    @Test
    public void testSocketPullOutOneProxy() throws Exception {
        
        ProxyUtil.getInstance().setCheckInterval(100);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertEquals(((ProxyInetSocketAddress)sa).down, false);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        Thread.sleep(100);
        //pull out one proxy
        Assert.assertEquals(((ProxyInetSocketAddress)sa).down , true);

        for(int i = 0; i < 1000; i++) {
            SocketAddress sa1 = ConnectionUtil.getAddress(socket, socketAddress);
            Assert.assertNotEquals(sa, sa1);
        }
        ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        ProxyUtil.getInstance().setChecker(null);
    }
    
    @Test
    public void testSocketPullOutAllProxy() throws Exception {
        ProxyUtil.getInstance().setCheckInterval(100);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        for(int i = 0; i < 2; i++) {
            SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
            try {
                ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
            } catch (Throwable t) {
                Assert.assertNotNull(t);
            }
            Thread.sleep(100);
        }
        ProxyUtil.getInstance().get(socketAddress).nextEndpoints().forEach(endpoint -> {
            Assert.assertEquals(endpoint.down, true); 
        });
        //Bottom-up plan
        SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertTrue(sa.equals(new ProxyInetSocketAddress(PROXY_IP_1, PROXY_PORT)) || sa.equals(new ProxyInetSocketAddress(PROXY_IP_2, PROXY_PORT)));
        ProxyUtil.getInstance().unregisterProxy(IP, PORT);
    }
    
    @Test 
    public void testSocketPullIn() throws InterruptedException {
        ProxyUtil.getInstance().setCheckInterval(100);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        for(int i = 0; i < 2; i++) {
            SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
            try {
                ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
            } catch (Throwable t) {
                Assert.assertNotNull(t);
            }
            Thread.sleep(100);
        }
        ProxyUtil.getInstance().get(socketAddress).nextEndpoints().forEach(endpoint -> {
            Assert.assertEquals(endpoint.down, true);
        });
        //pull out all proxy
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                if(address.equals(new ProxyInetSocketAddress(PROXY_IP_1, PROXY_PORT)) ) {
                    return CompletableFuture.completedFuture(true);
                } else {
                    return CompletableFuture.completedFuture(false);
                }
            }
        });
        Thread.sleep(100);
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        for(int i = 0; i < 100; i++) {
            ProxyInetSocketAddress sa1 = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
            Assert.assertEquals(sa, sa1);
        }
        ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        ProxyUtil.getInstance().setChecker(null);
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