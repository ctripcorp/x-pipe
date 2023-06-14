package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import com.ctrip.framework.xpipe.redis.proxy.ProxyResourceManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;
import static org.mockito.Mockito.*;

/**
 * @Author limingdong
 * @create 2021/4/22
 */
@RunWith(MockitoJUnitRunner.class)
public class ConnectionUtilTest extends AbstractProxyTest {

    @Mock
    private SocketChannel channel1;
    @Mock
    private SocketChannel channel2;
    final int defaultCheckInterval = 100;   
    protected void restoredProxyState() throws TimeoutException {
        ProxyUtil.getInstance().setCheckInterval(defaultCheckInterval);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(true);
            }
        });
        ProxyUtil.getInstance().startCheck();
        waitConditionUntilTimeOut(() -> {
            ProxyResourceManager manager = ProxyUtil.getInstance().get(socketAddress);
            if(manager == null) return true;
            return manager.nextEndpoints().stream().filter(
                    (endpoint) -> endpoint.sick
            ).collect(Collectors.toList()).size() == 0;
        }, defaultCheckInterval, 10);
        ProxyUtil.getInstance().setChecker(null);
        ProxyUtil.getInstance().stopCheck();
    }
    
    @Before
    public void setUp() throws IOException, InterruptedException, TimeoutException {
        super.setUp();
        restoredProxyState();
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
        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertEquals(((ProxyInetSocketAddress)sa).down, false);
        Assert.assertEquals(((ProxyInetSocketAddress)sa).sick, false);
        ProxyUtil.getInstance().setCheckInterval(defaultCheckInterval);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        ProxyUtil.getInstance().startCheck();
        
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        waitConditionUntilTimeOut(() -> ((ProxyInetSocketAddress)sa).down, defaultCheckInterval, 20);

        for(int i = 0; i < 1000; i++) {
            SocketAddress sa1 = ConnectionUtil.getAddress(socket, socketAddress);
            Assert.assertNotEquals(sa, sa1);
        }
        ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        ProxyUtil.getInstance().setChecker(null);
        ProxyUtil.getInstance().stopCheck();
    }
    
    @Test
    public void testSocketPullOutAllProxy() throws Exception {
        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        setAllProxyStatusDown(PROXY_SIZE, defaultCheckInterval);
        waitConditionUntilTimeOut(() -> ProxyUtil.getInstance().get(socketAddress).nextEndpoints().stream().filter(endpoint -> !endpoint.down).collect(Collectors.toList()).size() == 0, 110, 10);

        //Bottom-up plan
        SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
        Assert.assertTrue(sa.equals(new ProxyInetSocketAddress(PROXY_IP_1, PROXY_PORT)) || sa.equals(new ProxyInetSocketAddress(PROXY_IP_2, PROXY_PORT)));
        ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        ProxyUtil.getInstance().setChecker(null);
    }
    
    @Test 
    public void testSocketPullIn() throws InterruptedException, TimeoutException {
        ProxyUtil.getInstance().registerProxy(IP, PORT, ROUTE_INFO);
        setAllProxyStatusDown(PROXY_SIZE, defaultCheckInterval);
        waitConditionUntilTimeOut(() -> ProxyUtil.getInstance().get(socketAddress).nextEndpoints().stream().filter(endpoint -> !endpoint.down).collect(Collectors.toList()).size() == 0, 110, 20);
        
        ProxyUtil.getInstance().setCheckInterval(defaultCheckInterval);
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
        ProxyUtil.getInstance().startCheck();
        waitConditionUntilTimeOut(() -> ProxyUtil.getInstance().get(socketAddress).nextEndpoints().stream().filter(endpoint -> !endpoint.down).collect(Collectors.toList()).size() == 1, 110, 10);

        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        for(int i = 0; i < 100; i++) {
            ProxyInetSocketAddress sa1 = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
            Assert.assertEquals(sa, sa1);
        }
        ProxyUtil.getInstance().unregisterProxy(IP, PORT);
        ProxyUtil.getInstance().setChecker(null);
        ProxyUtil.getInstance().stopCheck();
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

    @Test
    public void testSocketChannelCheckTask() throws Exception {
        Mockito.when(channel1.finishConnect()).thenReturn(true);
        Mockito.when(channel2.finishConnect()).thenThrow(IOException.class);

        ConnectionUtil.socketChannelMap.put(channel1, new ReentrantLock());
        ConnectionUtil.socketChannelMap.put(channel2, new ReentrantLock());

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(new ConnectionUtil.SocketChannelMapCheckTask(), 0, TimeUnit.SECONDS);

        Thread.sleep(100L);
        Assert.assertEquals(ConnectionUtil.socketChannelMap.size(), 1);
        Assert.assertEquals(new ArrayList<>(ConnectionUtil.socketChannelMap.keySet()),
            Collections.singletonList(channel1));
    }

}