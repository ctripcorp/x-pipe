package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.framework.xpipe.redis.AllTests.IP;
import static com.ctrip.framework.xpipe.redis.AllTests.PORT;

/**
 * @Author limingdong
 * @create 2021/4/22
 */
public class ProxyUtilTest extends AbstractProxyTest {

    private String EXPECT_PROTOCOL = String.format("+PROXY ROUTE PROXYTLS://127.0.0.0:443 TCP://%s:%s;\r\n", IP, PORT);

    private ProxyUtil proxyUtil = ProxyUtil.getInstance();

    @Before
    public void setUp() {
        socket = new Socket();
        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
    }

    @After
    public void tearDown() throws Exception {
        proxyUtil.removeProxyAddress(socket);
        proxyUtil.unregisterProxy(IP, PORT);
    }

    @Test
    public void testProxyConnectProtocol() {
        Assert.assertFalse(proxyUtil.needProxy(new InetSocketAddress(IP, PORT + 1)));
        Assert.assertTrue(proxyUtil.needProxy(socketAddress));

        InetSocketAddress inetSocketAddress = proxyUtil.getProxyAddress(socket, socketAddress);
        Assert.assertNotNull(inetSocketAddress);
        Assert.assertTrue(inetSocketAddress.equals(new InetSocketAddress(PROXY_IP_1, PROXY_PORT)) || inetSocketAddress.equals(new InetSocketAddress(PROXY_IP_2, PROXY_PORT)));

        byte[] protocolBytes = proxyUtil.getProxyConnectProtocol(socket);
        String protocol = new String(protocolBytes);
        Assert.assertEquals(EXPECT_PROTOCOL, protocol);
    }
    
    @Test
    public void testSetChecker() throws InterruptedException {
        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
        proxyUtil.setCheckInterval(100);
        proxyUtil.setChecker(new ProxyChecker() {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        Thread.sleep(100);
        Assert.assertEquals(sa.down, true);
        Assert.assertEquals(sa.sick, true);
        proxyUtil.setChecker(new ProxyChecker() {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(true);
            }
        });
        Thread.sleep(100);
        Assert.assertEquals(sa.down, false);
        Assert.assertEquals(sa.sick, false);
        proxyUtil.unregisterProxy(IP, PORT);
    }

    @Test
    public void testSetCheckInterval() throws InterruptedException {
        proxyUtil.setCheckInterval(100);
        final AtomicLong counter =new AtomicLong(0);
        proxyUtil.setChecker(new ProxyChecker() {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                counter.addAndGet(1);
                return CompletableFuture.completedFuture(false);
            }
        });
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        Thread.sleep(150);
        Assert.assertEquals(counter.get(), 1);
        proxyUtil.setCheckInterval(500);
        Thread.sleep(550);
        Assert.assertEquals(counter.get() ,2);
        
    }

    @Test
    public void getOrCreateProxy() throws InterruptedException {
        int threadNum = 10;
        CountDownLatch cld = new CountDownLatch(threadNum);
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        cld.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    proxyUtil.getOrCreateProxy((InetSocketAddress) socketAddress);
                }
            });
            threads[i] = t;
            t.start();
            cld.countDown();
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }
        ProxyInetSocketAddress proxy = proxyUtil.getOrCreateProxy((InetSocketAddress) socketAddress);
        Assert.assertEquals(proxy.reference , threadNum + 1);

    }
    
    @Test 
    public void CreateAndDelProxy() throws InterruptedException {
        ProxyInetSocketAddress proxy = proxyUtil.getOrCreateProxy((InetSocketAddress) socketAddress);
        Assert.assertEquals(proxyUtil.removeProxy(proxy), proxy);
        
        int threadNum = 10;
        int runNum = 100;
        CountDownLatch cld = new CountDownLatch(threadNum);
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        cld.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for(int j = 0; j < runNum; j++) {
                        proxyUtil.removeProxy(proxyUtil.getOrCreateProxy((InetSocketAddress) socketAddress));
                    }
                }
            });
            threads[i] = t;
            t.start();
            cld.countDown();
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }
        proxy = proxyUtil.getOrCreateProxy((InetSocketAddress) socketAddress);
        Assert.assertEquals(proxy.reference ,  1);
    }
}