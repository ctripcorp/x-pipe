package com.ctrip.xpipe.redis.console.health;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.health.delay.DefaultDelayMonitor;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.OsUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jan 23, 2018
 */
public class RemoveUnusedRedisTest extends AbstractTest {

    private Server server;

    @InjectMocks
    private DefaultRedisSessionManager manager = new DefaultRedisSessionManager();

    @Mock
    private MetaCache metaCache;

    private int port;

    @Before
    public void beforeRemoveUnusedRedisTest() throws Exception {
        MockitoAnnotations.initMocks(this);

        // mock datas
        XpipeMeta xpipeMeta = new XpipeMeta().addDc(new DcMeta());
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        manager.executors = Executors.newFixedThreadPool(OsUtils.getCpuCount());
        manager.pingAndDelayExecutor = Executors.newFixedThreadPool(OsUtils.getCpuCount());

        // random port to avoid port conflict
        port = randomPort();
        server = startServer(port, new Callable<String>() {

            @Override
            public String call() throws Exception {
                return "+OK\r\n";
            }
        });
    }

    @Test
    public void testRemoveUnusedRedis() throws Exception {
        String host = "127.0.0.1";
        RedisSession session = manager.findOrCreateSession(host, port);

        // Build two types connection
        try {
            session.ping(new PingCallback() {
                @Override
                public void pong(String pongMsg) {

                }

                @Override
                public void fail(Throwable th) {

                }
            });
            session.subscribeIfAbsent(DefaultDelayMonitor.CHECK_CHANNEL, new RedisSession.SubscribeCallback() {
                @Override
                public void message(String channel, String message) {

                }

                @Override
                public void fail(Throwable e) {

                }
            });
        } catch (Exception ignore) {
            
        }
        // Wait for async call to establish connection
        waitConditionUntilTimeOut(() -> server.getConnected() == 2, 2000);
        Assert.assertEquals(2, server.getConnected());

        // Call remove redis session method
        manager.removeUnusedRedises();

        // Check if all connections are closed
        waitConditionUntilTimeOut(() -> server.getConnected() == 0, 3000);

        Assert.assertEquals(0, server.getConnected());
    }
}
