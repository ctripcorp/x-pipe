package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.session.PingCallback;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

public class RedisSessionResponseTimeTest extends AbstractConsoleIntegrationTest {

    private Server server;

    private RedisSession redisSession;

    private static final int COUNT = 200;

    private static final int TIMEOUT = 1000;

    private static final String CHECK_CHANNEL = "xpipe-health-check";

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Resource
    private CheckerConfig config;

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
        redisSession = new RedisSession(new DefaultEndPoint(HOST, BLOCKED_PORT), scheduled, getXpipeNettyClientKeyedObjectPool(), config);
    }

    @Test
    public void subscribeIfAbsent() throws Exception {

        long begin = System.currentTimeMillis();
        for(int i = 0; i < COUNT; i++) {
            redisSession.subscribeIfAbsent(new RedisSession.SubscribeCallback() {

                @Override
                public void message(String channel, String message) {
                    System.out.println("Pong");
                }

                @Override
                public void fail(Throwable e) {
                    System.out.println(e.getMessage());
                }
            }, CHECK_CHANNEL);
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
                public void role(String role, Role detail) {

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

    @After
    public void afterRedisSessionTest() throws Exception {
        server.stop();
    }
}