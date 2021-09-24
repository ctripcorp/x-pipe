package com.ctrip.xpipe.redis.console;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.client.reactive.ReactorResourceFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

/**
 * @author lishanglin
 * date 2021/9/18
 */
public class WebFluxTest extends AbstractConsoleTest {

    private MockWebServer webServer;

    private LoopResources loopResources;

    private ConnectionProvider connectionProvider;

    private WebClient client;

    private int connTimeoutMilli = 1000;
    private int readWriteTimeoutMilli = 3000;

    @Before
    public void setupDefaultRemoteCheckerManagerTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());

        loopResources = LoopResources.create("TestHttpLoop", LoopResources.DEFAULT_IO_WORKER_COUNT, true);
        connectionProvider = ConnectionProvider.builder("TestConnProvider").maxConnections(100)
                .pendingAcquireTimeout(Duration.ofMillis(1000))
                .maxIdleTime(Duration.ofMillis(1000)).build();
        client = makeWebClient("http://127.0.0.1:" + webServer.getPort(), connTimeoutMilli, readWriteTimeoutMilli, loopResources, connectionProvider);
    }


    public WebClient makeWebClient(String baseUrl, int connTimeout, int readWriteTimeout, LoopResources loopResources, ConnectionProvider connectionProvider) {
        ReactorResourceFactory resourceFactory = new ReactorResourceFactory();
        resourceFactory.setUseGlobalResources(false);
        resourceFactory.setConnectionProvider(connectionProvider);
        resourceFactory.setLoopResources(loopResources);

        return makeWebClient(baseUrl, connTimeout, readWriteTimeout, resourceFactory);
    }

    public WebClient makeWebClient(String baseUrl, int connTimeout, int readWriteTimeout, ReactorResourceFactory resourceFactory) {
        UnaryOperator<HttpClient> mapper = httpClient ->
                httpClient.tcpConfiguration(tcpClient ->
                        tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connTimeout)
                                .doOnConnected(connection ->
                                        connection.addHandlerLast(new ReadTimeoutHandler(readWriteTimeout, TimeUnit.MILLISECONDS))
                                                .addHandlerLast(new WriteTimeoutHandler(readWriteTimeout, TimeUnit.MILLISECONDS))));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(resourceFactory, mapper))
                .baseUrl(baseUrl)
                .build();
    }

    @Test
    public void testReactorIo() throws Exception {
        int reqCnt = 10;
        CountDownLatch latch = new CountDownLatch(reqCnt);
        IntStream.range(0, reqCnt).forEach(i -> {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody("pong").setBodyDelay(1, TimeUnit.SECONDS));
        });

        long start = System.currentTimeMillis();
        logger.info("[testNotBlockIo] start at {}", start);
        IntStream.range(0, reqCnt).forEach(i -> {
            client.get().uri("/ping")
                    .retrieve().bodyToMono(String.class)
                    .retry(3)
                    .subscribe(restResponse -> {
                        if (restResponse.equals("pong")) latch.countDown();
                        else logger.info("[testNotBlockIo] unexpected resp {}", restResponse);
                    });
        });

        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        long end = System.currentTimeMillis();
        logger.info("[testNotBlockIo] end at {}", end);
        Assert.assertTrue(end - start < 3000);
    }

}
