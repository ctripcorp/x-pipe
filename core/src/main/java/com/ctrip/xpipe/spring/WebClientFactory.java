package com.ctrip.xpipe.spring;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.client.reactive.ReactorResourceFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * @author lishanglin
 * date 2021/9/23
 */
public class WebClientFactory {

    public static final int MAX_CONNECT_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("connect-timeout", "1200"));
    public static final int MAX_SO_TIMEOUT_MILLI = Integer.parseInt(System.getProperty("so-timeout", "6000"));

    private WebClientFactory() {}

    public static WebClient makeWebClient(LoopResources loopResources, ConnectionProvider connectionProvider) {
        return makeWebClient(MAX_CONNECT_TIMEOUT_MILLI, MAX_SO_TIMEOUT_MILLI, loopResources, connectionProvider);
    }

    public static WebClient makeWebClient(int connTimeout, int soTimeout, LoopResources loopResources, ConnectionProvider connectionProvider) {
        return makeWebClient(connTimeout, soTimeout, soTimeout, loopResources, connectionProvider);
    }

    public static WebClient makeWebClient(int connTimeout, int readTimeout, int writeTimeout, LoopResources loopResources, ConnectionProvider connectionProvider) {
        ReactorResourceFactory resourceFactory = new ReactorResourceFactory();
        resourceFactory.setUseGlobalResources(false);
        resourceFactory.setConnectionProvider(connectionProvider);
        resourceFactory.setLoopResources(loopResources);

        return makeWebClient(connTimeout, readTimeout, writeTimeout, resourceFactory);
    }

    public static WebClient makeWebClient(int connTimeout, int soTimeout, ReactorResourceFactory resourceFactory) {
        return makeWebClient(connTimeout, soTimeout, soTimeout, resourceFactory);
    }

    public static WebClient makeWebClient(int connTimeout, int readTimeout, int writeTimeout, ReactorResourceFactory resourceFactory) {
        UnaryOperator<HttpClient> mapper = httpClient ->
                httpClient.tcpConfiguration(tcpClient ->
                        tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connTimeout)
                                .doOnConnected(connection ->
                                        connection.addHandlerLast(new ReadTimeoutHandler(readTimeout, TimeUnit.MILLISECONDS))
                                                .addHandlerLast(new WriteTimeoutHandler(writeTimeout, TimeUnit.MILLISECONDS))));

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(resourceFactory, mapper))
                .build();
    }

}
