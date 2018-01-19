package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.simpleserver.Server;
import com.lambdaworks.redis.ClientOptions;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.SocketOptions;
import com.lambdaworks.redis.resource.DefaultClientResources;
import com.lambdaworks.redis.resource.Delay;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class RedisSessionTest extends AbstractConsoleIntegrationTest {

    private Server server;

    private RedisSession redisSession;

    private static final int COUNT = 200;

    private static final int TIMEOUT = 1000;

    private static final String CHECK_CHANNEL = "xpipe-health-check";

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Before
    public void beforeRedisSessionTest() throws Exception {
        int BLOCKED_PORT = 55555;
        int BLOCK_TIME = 1000 * 20;
        String HOST = "127.0.0.1";
        server = startServer(BLOCKED_PORT, new Callable<String>() {

            @Override
            public String call() throws Exception {
                sleep(BLOCK_TIME);
                return "+OK\r\n";
            }
        });
        redisSession = new RedisSession(createRedisClient(HOST, BLOCKED_PORT),
                new HostPort(HOST, BLOCKED_PORT), executors, executors);
    }

    @Test
    public void subscribeIfAbsent() throws Exception {

        long begin = System.currentTimeMillis();
        for(int i = 0; i < COUNT; i++) {
            redisSession.subscribeIfAbsent(CHECK_CHANNEL, new RedisSession.SubscribeCallback() {

                @Override
                public void message(String channel, String message) {
                    System.out.println("Pong");
                }

                @Override
                public void fail(Exception e) {
                    System.out.println(e.getMessage());
                }
            });
        }
        long after = System.currentTimeMillis();
        Assert.assertTrue(after - begin < TIMEOUT);
    }

    @Test
    public void publish() throws Exception {
        long begin = System.currentTimeMillis();
        for(int i = 0; i < COUNT; i++) {
            redisSession.publish(CHECK_CHANNEL, CHECK_CHANNEL);
        }
        long after = System.currentTimeMillis();
        Assert.assertTrue(after - begin < TIMEOUT);
    }

    @Test
    public void ping() throws Exception {
        long begin = System.currentTimeMillis();
        for(int i = 0; i < COUNT; i++) {
            redisSession.ping(new PingCallback() {
                @Override
                public void pong(String pongMsg) {
                    System.out.println("pong");
                }

                @Override
                public void fail(Throwable th) {

                }
            });
        }
        long after = System.currentTimeMillis();
        Assert.assertTrue(after - begin < TIMEOUT);
    }

    @Test
    public void role() throws Exception {
        long begin = System.currentTimeMillis();
        for(int i = 0; i < COUNT; i++) {
            redisSession.role(new RedisSession.RollCallback() {
                @Override
                public void role(String role) {

                }

                @Override
                public void fail(Throwable e) {

                }
            });
        }
        long after = System.currentTimeMillis();
        Assert.assertTrue(after - begin < TIMEOUT);
    }

    @Test
    public void configRewrite() throws Exception {
        long begin = System.currentTimeMillis();
        for(int i = 0; i < COUNT; i++) {
            redisSession.configRewrite((str, th) -> {});
        }
        long after = System.currentTimeMillis();
        System.out.println(begin + " : " + after);
        Assert.assertTrue(after - begin < TIMEOUT);
    }

    private RedisClient createRedisClient(String host, int port) {
        RedisURI redisUri = new RedisURI(host, port, 2, TimeUnit.SECONDS);
        SocketOptions socketOptions = SocketOptions.builder()
                .connectTimeout(XPipeConsoleConstant.SOCKET_TIMEOUT, TimeUnit.SECONDS)
                .build();
        ClientOptions clientOptions = ClientOptions.builder() //
                .socketOptions(socketOptions)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)//
                .build();

        DefaultClientResources clientResources = DefaultClientResources.builder()//
                .reconnectDelay(Delay.constant(1, TimeUnit.SECONDS))//
                .build();
        RedisClient redis = RedisClient.create(clientResources, redisUri);
        redis.setOptions(clientOptions);
        return redis;
    }




    @After
    public void afterRedisSessionTest() throws Exception {
        server.stop();
    }
}