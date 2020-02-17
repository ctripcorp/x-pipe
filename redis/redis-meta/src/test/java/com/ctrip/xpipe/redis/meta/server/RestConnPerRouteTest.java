package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class RestConnPerRouteTest extends AbstractMetaServerTest {

    private int PORT = randomPort();

    private static final int serverDelay = 1000;

    private Server server;

    @Before
    public void setUpRestConnPerRouteTest() throws Exception {
        server = startServer(PORT, new Callable<String>() {
            @Override
            public String call() throws Exception {
                sleep(serverDelay);
                return "HTTP/1.1 200 OK\r\n"
                        + "Content-type:text/plain\r\n"
                        + "Content-length:4\r\n" +
                        "\r\n"
                        + "OK\r\n";
            }
        });
    }

    @After
    public void afterRestConnPerRouteTest() throws Exception {
        server.stop();
    }

    @Test
    public void testForSingleConn() throws Exception {
        RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(1, 10, 5000, 5000);
        long startAt = System.currentTimeMillis();

        Assert.assertTrue(concurrentRequest(restTemplate, 2, 4 * serverDelay));
        Assert.assertTrue(System.currentTimeMillis() - startAt > 2 * serverDelay);
    }

    @Test
    public void testFroMultiConn() throws Exception {
        RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(5, 10, 5000, 5000);
        Assert.assertTrue(concurrentRequest(restTemplate, 5, 2 * serverDelay));
    }

    private boolean concurrentRequest(RestOperations restTemplate, int threadCnt, int timeoutMillSecond) throws Exception {
        CountDownLatch latch = new CountDownLatch(threadCnt);
        IntStream.range(0, threadCnt).forEach(i -> {
            new Thread(() -> {
                restTemplate.getForEntity("http://127.0.0.1:" + PORT, String.class);
                latch.countDown();
            }).start();
        });
        return latch.await(timeoutMillSecond, TimeUnit.MILLISECONDS);
    }

}
