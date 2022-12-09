package com.ctrip.xpipe.redis.integratedtest.console;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.TestConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.session.Callbackable;
import com.ctrip.xpipe.redis.checker.healthcheck.session.PingCallback;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.PublishCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.pubsub.SubscribeListener;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.proxy.parser.DefaultProxyConnectProtocolParser;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
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
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(redisPort);
        startRedis(redisMeta);
        endpoint = new DefaultEndPoint(redisMeta.getIp(), redisMeta.getPort());
        redisSession = new RedisSession(endpoint, scheduled, getReqResNettyClientPool(), new TestConfig());
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
        waitConditionUntilTimeOut(()->result.get() != null, 5000);
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
        redisSession.subscribeIfAbsent(new RedisSession.SubscribeCallback() {
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
        }, SUBSCRIBE_CHANNEL);
        Thread.sleep(1000);
        redisSession.publish(SUBSCRIBE_CHANNEL, message);
        waitConditionUntilTimeOut(()->result.get()!=null, 2000);
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
            public void role(String role, Role detail) {
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
        redisSession.info("replication", new Callbackable<String>() {
            @Override
            public void success(String message) {
                logger.info("[info] message: {}", message);
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

    @Test
    public void testPingThroughProxy() throws Exception {
        String protocolStr = "PROXY ROUTE PROXYTCP://10.5.111.164:80 TCP://10.5.111.145:6379";
        ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read(protocolStr);
        endpoint = new DefaultEndPoint("10.5.111.145", 6379, protocol);
        redisSession = new RedisSession(endpoint, scheduled, getReqResNettyClientPool(), new TestConfig());
        redisSession.ping(new PingCallback() {
            @Override
            public void pong(String pongMsg) {
                logger.info("[ping-call-back] {}", pongMsg);
            }

            @Override
            public void fail(Throwable th) {
                logger.error("[ping-call-back]", th);
            }
        });
        Thread.sleep(10000);
    }

    @Test
    public void testSubThroughProxy() throws Exception {
        String protocolStr = "PROXY ROUTE PROXYTCP://10.5.111.148:80 TCP://10.5.111.145:6379";
        ProxyConnectProtocol protocol = new DefaultProxyConnectProtocolParser().read(protocolStr);
        endpoint = new DefaultEndPoint("10.5.111.145", 6379, protocol);
        redisSession = new RedisSession(endpoint, scheduled, getReqResNettyClientPool(), new TestConfig());
        redisSession.subscribeIfAbsent(new RedisSession.SubscribeCallback() {
            @Override
            public void message(String channel, String message) {
                logger.info("[receive] channel: {}, message: {}", channel, message);
            }

            @Override
            public void fail(Throwable e) {

            }
        }, SUBSCRIBE_CHANNEL);
        Thread.sleep(10);
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world1");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world2");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world3");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world4");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world5");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world6");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world7");
        Thread.sleep(200);
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world8");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world9");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world10");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world11");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world12");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world13");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world14");
        redisSession.publish(SUBSCRIBE_CHANNEL, "hello-world15");
        Thread.sleep(5000);
        redisSession.closeSubscribedChannel(SUBSCRIBE_CHANNEL);
        Thread.sleep(1000);
    }

    public XpipeNettyClientKeyedObjectPool getReqResNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory());
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    public XpipeNettyClientKeyedObjectPool getSubscribeNettyClientPool() throws Exception {
        XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory());
        LifecycleHelper.initializeIfPossible(keyedObjectPool);
        LifecycleHelper.startIfPossible(keyedObjectPool);
        return keyedObjectPool;
    }

    private NettyKeyedPoolClientFactory getKeyedPoolClientFactory() {
        return new NettyKeyedPoolClientFactory();
    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {
        return null;
    }
}
