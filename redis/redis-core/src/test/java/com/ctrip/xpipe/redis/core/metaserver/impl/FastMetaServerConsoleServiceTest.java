package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class FastMetaServerConsoleServiceTest extends AbstractTest {

    private Server metaServer;

    private FastMetaServerConsoleService fastMetaServerConsoleService;

    private static final String HTTP_RESP_TEMP = "HTTP/1.1 200 OK\r\n" +
            "Content-Type: application/json;charset=UTF-8\r\n" +
            "\r\n%s";

    private AtomicBoolean isHang = new AtomicBoolean(false);

    private int delta = 200;

    private int readDelta = 600;

    @Before
    public void setupFastMetaServerConsoleServiceTest() throws Exception {
        metaServer = startServer(randomPort(), new Function<String, String>() {
            @Override
            public String apply(String s) {
                if (isHang.get()) sleep(1000);
                return mockHttpResp("{\"ip\":\"127.0.0.1\", \"port\": 6379}");
            }
        });
        fastMetaServerConsoleService = new FastMetaServerConsoleService("http://127.0.0.1:" + metaServer.getPort());
    }

    private String mockHttpResp(String content) {
        return String.format(HTTP_RESP_TEMP, content);
    }

    @After
    public void afterFastMetaServerConsoleServiceTest() throws Exception {
        if (null != metaServer) metaServer.stop();
    }

    @Test
    public void testRespOnTime() {
        RedisMeta result = fastMetaServerConsoleService.getCurrentMaster("cluster1", "shard1");
        Assert.assertEquals("127.0.0.1", result.getIp());
        Assert.assertEquals(6379, result.getPort().intValue());
    }

    @Test
    public void testConnectTimeout() throws Exception {
        metaServer.stop();
        metaServer = null;
        long start = System.currentTimeMillis();
        try {
            RedisMeta result = fastMetaServerConsoleService.getCurrentMaster("cluster1", "shard1");
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof ConnectException);
            Assert.assertTrue(AbstractService.FAST_CONNECT_TIMEOUT + delta > System.currentTimeMillis() - start);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testReadTimeout() {
        isHang.set(true);
        long start = System.currentTimeMillis();
        try {
            RedisMeta result = fastMetaServerConsoleService.getCurrentMaster("cluster1", "shard1");
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof SocketTimeoutException);
            Assert.assertTrue(duration > AbstractService.FAST_SO_TIMEOUT);
            Assert.assertTrue(AbstractService.DEFAULT_SO_TIMEOUT > duration);
            return;
        }
        Assert.fail();
    }

}
