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
import java.util.concurrent.TimeoutException;
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

    final int defaultCheckInterval = 100;
    final int defaultCheckWait = 5000;
    
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
    public void testProxyConnectProtocolWithRegisterKey() {
        proxyUtil.removeProxyAddress(socket);
        proxyUtil.unregisterProxy(IP, PORT);

        String registerKey1 = "registerKey1";
        String registerKey2 = "registerKey2";
        proxyUtil.registerProxy(registerKey1, IP, PORT, ROUTE_INFO);
        Assert.assertTrue(proxyUtil.needProxy(socketAddress));

        proxyUtil.registerProxy(registerKey2, IP, PORT, ROUTE_INFO);
        proxyUtil.unregisterProxy(registerKey1, IP, PORT);
        Assert.assertTrue(proxyUtil.needProxy(socketAddress));

        proxyUtil.unregisterProxy(registerKey2, IP, PORT);
        Assert.assertFalse(proxyUtil.needProxy(socketAddress));
    }

    @Test
    public void testProxyConnectProtocolWithMixedRegister() {
        proxyUtil.removeProxyAddress(socket);
        proxyUtil.unregisterProxy(IP, PORT);

        String registerKey1 = "registerKey1";
        proxyUtil.registerProxy(registerKey1, IP, PORT, ROUTE_INFO);
        Assert.assertTrue(proxyUtil.needProxy(socketAddress));
        proxyUtil.unregisterProxy(IP, PORT);
        Assert.assertFalse(proxyUtil.needProxy(socketAddress));

        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
        Assert.assertTrue(proxyUtil.needProxy(socketAddress));
        proxyUtil.unregisterProxy(registerKey1, IP, PORT);
        Assert.assertFalse(proxyUtil.needProxy(socketAddress));
    }

    @Test
    public void testSetChecker() throws InterruptedException, TimeoutException {
        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
        proxyUtil.setCheckInterval(defaultCheckInterval);
        proxyUtil.setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        proxyUtil.startCheck();
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        
        waitConditionUntilTimeOut(() -> sa.down && sa.sick, defaultCheckInterval * 2, 10);
        
        proxyUtil.setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(true);
            }
        });
        waitConditionUntilTimeOut(() -> !sa.down && !sa.sick , 100, 10);
        proxyUtil.unregisterProxy(IP, PORT);
        proxyUtil.setChecker(null);
        proxyUtil.stopCheck();
    }
    
    @Test
    public void testCheckerUp() throws InterruptedException, TimeoutException {
        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
        proxyUtil.setCheckInterval(100);
        proxyUtil.setChecker(new AbstractProxyCheckerTest(10,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        proxyUtil.startCheck();
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        waitConditionUntilTimeOut(() -> sa.down && sa.sick, 110, 10);
        proxyUtil.setChecker(new AbstractProxyCheckerTest(10,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(true);
            }
        });
        Assert.assertEquals(sa.down, true);
        Assert.assertEquals(sa.sick, true);
        waitConditionUntilTimeOut(() -> !sa.down && !sa.sick, 1100, 100);
        proxyUtil.unregisterProxy(IP, PORT);
        proxyUtil.setChecker(null);
        proxyUtil.stopCheck();
    }
    
    @Test
    public void testCheckerDown() throws InterruptedException, TimeoutException {
        proxyUtil.registerProxy(IP, PORT, ROUTE_INFO);
        proxyUtil.setCheckInterval(100);
        proxyUtil.setChecker(new AbstractProxyCheckerTest(1,10) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        proxyUtil.startCheck();
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        Assert.assertEquals(sa.down, false);
        Assert.assertEquals(sa.sick, true);
        waitConditionUntilTimeOut(() -> sa.down && sa.sick, 1100, 100);
        proxyUtil.setChecker(null);
        proxyUtil.stopCheck();
    }

    @Test
    public void testSetCheckInterval() throws InterruptedException, TimeoutException {
        proxyUtil.setCheckInterval(100);
        final AtomicLong counter =new AtomicLong(0);
        proxyUtil.setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                counter.addAndGet(1);
                return CompletableFuture.completedFuture(false);
            }
        });
        proxyUtil.startCheck();
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        long current = counter.get();
        long finalCurrent = current;
        waitConditionUntilTimeOut(() -> counter.get() == (finalCurrent + 1), 110, 10);
        proxyUtil.setCheckInterval(500);
        current = counter.get();
        long finalCurrent1 = current;
        waitConditionUntilTimeOut(() -> counter.get() == (finalCurrent1 + 1));
        proxyUtil.setChecker(null);
        proxyUtil.stopCheck();

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
        Assert.assertEquals(proxy.reference.get() , threadNum + 1);
        for(int i = 0; i < threadNum +1; i++) {
            proxyUtil.removeProxy(proxy);
        }

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
        Assert.assertEquals(proxy.reference.get() ,  1);
        proxyUtil.removeProxy(proxy);
    }
    
    @Test 
    public void testProxyUpEvent() throws TimeoutException {
        final AtomicLong counter =new AtomicLong(0);
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        setAllProxyStatusDown(PROXY_SIZE, defaultCheckInterval);
        waitConditionUntilTimeOut(() -> sa.down, defaultCheckInterval, 10);
        proxyUtil.onProxyUp(proxyInetSocketAddress -> {
            Assert.assertEquals(proxyInetSocketAddress, sa);
            counter.addAndGet(1);
        });
        proxyUtil.setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                if(address.equals(sa)) {
                    return CompletableFuture.completedFuture(true);    
                } else {
                    return CompletableFuture.completedFuture(false);
                }
            }
        });
        proxyUtil.startCheck();
        waitConditionUntilTimeOut(() -> !sa.down, defaultCheckWait, defaultCheckInterval);
        waitConditionUntilTimeOut(() -> counter.get() == 1, defaultCheckWait, defaultCheckInterval);
        proxyUtil.setChecker(null);
        proxyUtil.stopCheck();
    }
    
    @Test 
    public void testProxyDownEvent() throws TimeoutException {
        proxyUtil.setCheckInterval(defaultCheckInterval);
        final AtomicLong counter =new AtomicLong(0);
        ProxyInetSocketAddress sa = (ProxyInetSocketAddress)ConnectionUtil.getAddress(socket, socketAddress);
        try {
            ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
        } catch (Throwable t) {
            Assert.assertNotNull(t);
        }
        proxyUtil.onProxyDown(proxyInetSocketAddress -> {
            Assert.assertEquals(proxyInetSocketAddress, sa);
            counter.addAndGet(1);
        });
        proxyUtil.setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        proxyUtil.startCheck();
        waitConditionUntilTimeOut(() -> sa.down, defaultCheckWait, defaultCheckInterval);
        waitConditionUntilTimeOut(() -> counter.get() == 1, defaultCheckWait, defaultCheckInterval);
        proxyUtil.setChecker(null);
        proxyUtil.stopCheck();
    }
}