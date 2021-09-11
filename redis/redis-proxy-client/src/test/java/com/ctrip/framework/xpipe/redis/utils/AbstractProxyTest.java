package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.proxy.ProxyInetSocketAddress;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

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

    protected static final int PROXY_SIZE = 2;
    
    protected SocketAddress socketAddress = new InetSocketAddress(IP, PORT);

    protected Socket socket;

    @Before
    public void setUp() throws IOException, InterruptedException, TimeoutException {
        socket = new Socket();
    }

    protected void waitConditionUntilTimeOut(BooleanSupplier booleanSupplier, int waitTimeMilli, int intervalMilli) throws TimeoutException {

        long maxTime = System.currentTimeMillis() + waitTimeMilli;


        while (true) {
            boolean result = booleanSupplier.getAsBoolean();
            if (result) {
                return;
            }
            if (System.currentTimeMillis() >= maxTime) {
                throw new TimeoutException("timeout still false:" + waitTimeMilli);
            }
            sleep(intervalMilli);
        }
    }

    protected void sleep(int miliSeconds) {

        try {
            TimeUnit.MILLISECONDS.sleep(miliSeconds);
        } catch (InterruptedException e) {
        }
    }

    protected  void setAllProxyStatusDown(int num, int checkInterval) throws TimeoutException {
        ProxyUtil.getInstance().setCheckInterval(checkInterval);
        ProxyUtil.getInstance().setChecker(new AbstractProxyCheckerTest(1,1) {
            @Override
            public CompletableFuture<Boolean> check(InetSocketAddress address) {
                return CompletableFuture.completedFuture(false);
            }
        });
        for(int i = 0; i < num; i++) {
            SocketAddress sa = ConnectionUtil.getAddress(socket, socketAddress);
            try {
                ConnectionUtil.connectToProxy(socket, (InetSocketAddress) sa, 500);
            } catch (Throwable t) {
                Assert.assertNotNull(t);
            }
            waitConditionUntilTimeOut(() -> ((ProxyInetSocketAddress)sa).down, checkInterval * 2, 10);
        }
        ProxyUtil.getInstance().setChecker(null);
    }
}
