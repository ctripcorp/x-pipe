package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleDbTest;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayAction;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.PingCallback;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
public class RemoveUnusedRedisTest extends AbstractConsoleDbTest {

    private Server server;

    @InjectMocks
    private DefaultRedisSessionManager manager = new DefaultRedisSessionManager();

    @Mock
    private MetaCache metaCache;

    private DefaultHealthCheckEndpointFactory endpointFactory;

    private int port;

    @Before
    public void beforeRemoveUnusedRedisTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        DefaultRedisSessionManager.checkUnusedRedisDelaySeconds = 2;

        // mock datas
        when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        logger.info("[xpipeMeta] {}", getXpipeMeta());
        when(metaCache.getRoutes()).thenReturn(null);

        // random port to avoid port conflict
        port = randomPort();
        server = startServer(port, new Callable<String>() {

            @Override
            public String call() throws Exception {
                return "+OK\r\n";
            }
        });

        endpointFactory = new DefaultHealthCheckEndpointFactory();
        endpointFactory.setMetaCache(metaCache);
        manager.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool());
        manager.setEndpointFactory(endpointFactory);
        manager.setScheduled(scheduled);
        manager.postConstruct();
    }

    @Test
    public void testRemoveUnusedRedis() throws Exception {
        String host = "127.0.0.1";
        RedisSession session = manager.findOrCreateSession(new HostPort(host, port));

        // Build two types connection
        // if ping first, subscribe will reuse connection for ping
        try {
            session.subscribeIfAbsent(DelayAction.CHECK_CHANNEL, new RedisSession.SubscribeCallback() {
                @Override
                public void message(String channel, String message) {

                }

                @Override
                public void fail(Throwable e) {

                }
            });
            session.ping(new PingCallback() {
                @Override
                public void pong(String pongMsg) {

                }

                @Override
                public void fail(Throwable th) {

                }
            });
        } catch (Exception e) {
            logger.info("[testRemoveUnusedRedis] connect fail", e);
        }
        // Wait for async call to establish connection
        waitConditionUntilTimeOut(() -> server.getConnected() == 2, 2000);
        Assert.assertEquals(2, server.getConnected());

        // Call remove redis session method
//        manager.removeUnusedRedises();

        // Check if all connections are closed
        // only close subscribe connection, is there a problem for not closing all connection?
        waitConditionUntilTimeOut(() -> server.getConnected() == 1, 3000);

        Assert.assertEquals(1, server.getConnected());
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "dc-meta-test.xml";
    }

}
