package com.ctrip.xpipe.redis.integratedtest.console;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.console.health.PingCallback;
import com.ctrip.xpipe.redis.console.health.RedisSession;
import com.ctrip.xpipe.redis.console.health.redisconf.Callbackable;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.PingCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.PublishCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeListener;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author chen.zhu
 * <p>
 * Aug 04, 2018
 */
public class RedisSessionTest extends AbstractIntegratedTest {

    private RedisSession redisSession;

    private Endpoint endpoint;

    private static final String SUBSCRIBE_CHANNEL = "xpipe-health-check-test";

    @Before
    public void beforeRedisSessionTest() throws Exception {
        int redisPort = randomPort();
        startRedis(new RedisMeta().setIp("127.0.0.1").setPort(redisPort));
        endpoint = localhostEndpoint(redisPort);
        redisSession = new RedisSession(endpoint, scheduled, getXpipeNettyClientKeyedObjectPool(), getXpipeNettyClientKeyedObjectPool());
    }

    @Test
    public void testSubscribe() throws Exception {
        logger.info("[testSubscribe] redis endpoint: {}", endpoint);
        final String message = "final-message";
        AtomicReference<String> result = new AtomicReference<>();
        SubscribeCommand subscribeCommand = new SubscribeCommand(endpoint, scheduled, SUBSCRIBE_CHANNEL);
        subscribeCommand.addChannelListener(new SubscribeListener() {
            @Override
            public void message(String channel, String message) {
                result.set(message);
                logger.info("[recived message] {}", message);
            }
        });
        subscribeCommand.execute();
        Thread.sleep(2000);
        logger.info("[testPublishCommand] redis endpoint: {}", endpoint);
        PublishCommand publishCommand = new PublishCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(endpoint),
                scheduled, SUBSCRIBE_CHANNEL, message);
        publishCommand.execute();
        waitConditionUntilTimeOut(()->result.get() != null, 1000);
        logger.info("[message] result: {}", result.get());
        Assert.assertEquals(message, result.get());
    }

    @Test
    public void testPublishCommand() throws Exception {
        logger.info("[testPublishCommand] redis endpoint: {}", endpoint);
        final String message = "final-message";
        PublishCommand publishCommand = new PublishCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(endpoint),
                scheduled, SUBSCRIBE_CHANNEL, message);
        publishCommand.execute().get();
    }

    @Test
    public void testSubscribeIfAbsent() throws TimeoutException, InterruptedException {
        AtomicReference<String> result = new AtomicReference<>();
        AtomicBoolean recieved = new AtomicBoolean(false);
        final String message = "hello-world";
        redisSession.subscribeIfAbsent(SUBSCRIBE_CHANNEL, new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                result.set(message);
                recieved.getAndSet(true);
                logger.info("[message] channel: {}, message: {}", channel, message);
            }

            @Override
            public void fail(Throwable e) {
                recieved.getAndSet(true);
                logger.error("[fail] cause: ", e);
            }
        });
        Thread.sleep(1000);
        redisSession.publish(SUBSCRIBE_CHANNEL, message);
        Thread.sleep(1000);
        Assert.assertNotNull(result.get());
        Assert.assertEquals(message, result.get());
    }

    @Test
    public void testPublish() {
        final String message = "hello-world";
        redisSession.publish(SUBSCRIBE_CHANNEL, message);
    }

    @Test
    public void testPing() throws InterruptedException {
        redisSession.ping(new PingCallback() {
            @Override
            public void pong(String pongMsg) {
                logger.info("[ping] success: {}", pongMsg);
            }

            @Override
            public void fail(Throwable th) {
                logger.error("[ping] fail: ", th);
            }
        });
        Thread.sleep(1000);
    }

    @Test
    public void testRole() throws InterruptedException {
        redisSession.role(new RedisSession.RollCallback() {
            @Override
            public void role(String role) {
                logger.info("[role] success: {}", role);
            }

            @Override
            public void fail(Throwable e) {
                logger.error("[role] fail: ", e);
            }
        });
        Thread.sleep(1000);
    }

    @Test
    public void testConfigRewrite() throws InterruptedException {
        redisSession.configRewrite((msg, th) -> {
            logger.info("[configRewrite] {}", msg);
        });
        Thread.sleep(1000);
    }

    @Test
    public void testRoleSync() throws Exception {
        String message = redisSession.roleSync();
        logger.info("[role sync] {}", message);
    }

    @Test
    public void testInfo() throws InterruptedException {
        redisSession.info("server", new Callbackable<String>() {
            @Override
            public void success(String message) {
                logger.info("[info] message: ", message);
            }

            @Override
            public void fail(Throwable throwable) {
                logger.error("[info]", throwable);
            }
        });
        Thread.sleep(1000);
    }

    @Test
    public void testInfoServer() throws InterruptedException {
        redisSession.infoServer(new Callbackable<String>() {
            @Override
            public void success(String message) {
                logger.info("[info] message: ", message);
            }

            @Override
            public void fail(Throwable throwable) {
                logger.error("[info]", throwable);
            }
        });
        Thread.sleep(1000);
    }

    @Test
    public void testInfoReplication() throws InterruptedException {
        redisSession.infoReplication(new Callbackable<String>() {
            @Override
            public void success(String message) {
                logger.info("[info] message: ", message);
            }

            @Override
            public void fail(Throwable throwable) {
                logger.error("[info]", throwable);
            }
        });
        Thread.sleep(1000);
    }

    @Test
    public void testIsDiskLess() {
        redisSession.isDiskLessSync(new Callbackable<Boolean>() {
            @Override
            public void success(Boolean message) {
                logger.info("[isDiskLessSync] {}", message);
            }

            @Override
            public void fail(Throwable throwable) {
                logger.error("[isDiskLessSync]", throwable);
            }
        });
    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {
        return null;
    }
}
