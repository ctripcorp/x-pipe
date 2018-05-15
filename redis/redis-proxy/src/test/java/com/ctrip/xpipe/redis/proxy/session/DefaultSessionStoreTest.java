package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * May 14, 2018
 */
public class DefaultSessionStoreTest extends AbstractRedisProxyServerTest {

    private DefaultSessionStore sessionStore;

    @Before
    public void beforeDefaultSessionStoreTest() throws Exception {
        sessionStore = (DefaultSessionStore) ((DefaultTunnel)tunnel()).getSessionStore();
    }

    @Test
    public void testTunnel() {
        Assert.assertNotNull(sessionStore.tunnel());
    }

    @Test
    public void testSession() {
        Session session = sessionStore.frontend();
        Assert.assertEquals(session, sessionStore.session(session.getChannel()));
    }

    @Test
    public void testFrontend() {
        sessionStore.frontend();
    }

    @Test
    public void testBackend() {
        sessionStore.backend();
    }

    @Test
    public void testGetOppositeSession() {
        Session frontend = sessionStore.frontend();
        Assert.assertEquals(frontend, sessionStore.getOppositeSession(sessionStore.backend()));
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testestGetOppositeSession2() {
//        sessionStore.frontend();
//        sessionStore.backend();
        sessionStore.getOppositeSession(null);
    }

    @Test
    public void testGetSessions() {
        sessionStore.frontend();
        sessionStore.backend();

        List<Session> result = sessionStore.getSessions();

        Assert.assertEquals(2, result.size());
    }
}