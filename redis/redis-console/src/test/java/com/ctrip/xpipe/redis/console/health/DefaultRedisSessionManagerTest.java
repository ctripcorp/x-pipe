package com.ctrip.xpipe.redis.console.health;

import com.ctrip.framework.xpipe.redis.ProxyChecker;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.TestConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         May 10, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultRedisSessionManagerTest extends AbstractConsoleTest{

    private DefaultRedisSessionManager redisSessionManager;
    private DefaultHealthCheckEndpointFactory endpointFactory;
    private CheckerConfig checkerConfig;

    private String host = "127.0.0.1";
    private int port = 6379;
    private String channel = "channel";
    private int subscribeTimeoutSeconds = 5;
    private int channels = 3;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ProxyChecker proxyChecker;

    private RouteChooseStrategyFactory routeChooseStrategyFactory = new DefaultRouteChooseStrategyFactory();

    @Before
    public void beforeDefaultRedisSessionManagerTest() throws Exception {

        redisSessionManager = new DefaultRedisSessionManager();
        checkerConfig = new TestConfig();
        Mockito.when(metaCache.getCurrentDcConsoleRoutes()).thenReturn(null);
        endpointFactory = new DefaultHealthCheckEndpointFactory(proxyChecker, checkerConfig, metaCache, routeChooseStrategyFactory);
        endpointFactory.setMetaCache(metaCache);
        redisSessionManager.setScheduled(scheduled);
        redisSessionManager.setEndpointFactory(endpointFactory);
        redisSessionManager.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool());
        redisSessionManager.setConfig(checkerConfig);
        redisSessionManager.postConstruct();
    }

    @Test
    public void testPubSub(){

        RedisSession redisSession = redisSessionManager.findOrCreateSession(new HostPort(host, port));

        for(int i=0;i<channels;i++){
            redisSession.subscribeIfAbsent(new RedisSession.SubscribeCallback() {
                @Override
                public void message(String channel, String message) {
                    logger.info("[message]{}, {}", channel, message);
                }
                @Override
                public void fail(Throwable e) {
                    logger.error("[fail]", e);
                }
            }, channelName(channel, i));
        }


        sleep(subscribeTimeoutSeconds * 2 * 1000);

        scheduled.scheduleAtFixedRate(new AbstractExceptionLogTask() {

            @Override
            public void doRun() {

                for(int i=0;i<channels;i++){
                    redisSession.publish(channelName(channel, i), randomString(10));
                }
            }
        }, 0, 5, TimeUnit.SECONDS);

    }

    private String channelName(String channel, int index) {

        return channel + "-" + index;
    }


    @After
    public void afterDefaultRedisSessionManagerTest() throws IOException {
        waitForAnyKey();
    }


}
