package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.session.state.SessionEstablished;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;


/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class DefaultSessionStatsTest extends AbstractRedisProxyServerTest {

    private DefaultSessionStats sessionStats;

    private final static int FIXED_INCREASE = 10000;

    @Before
    public void beforeDefaultSessionStatsTest() {
        Session session = Mockito.mock(Session.class);
        SessionEstablished sessionEstablished = Mockito.mock(SessionEstablished.class);
        Mockito.when(session.getSessionState()).thenReturn(sessionEstablished);
        sessionStats = new DefaultSessionStats(session, scheduled);
    }

    @Test
    public void testIncreaseInputBytes() {
        int N = 10;
        for(int i = 0; i < N; i++) {
            sessionStats.increaseInputBytes(FIXED_INCREASE);
        }
        Assert.assertEquals(N * FIXED_INCREASE, sessionStats.getInputBytes());
    }

    @Test
    public void testIncreaseOutputBytes() {
        int N = 10;
        for(int i = 0; i < N; i++) {
            sessionStats.increaseOutputBytes(FIXED_INCREASE);
        }
        Assert.assertEquals(N * FIXED_INCREASE, sessionStats.getOutputBytes());
    }

    @Test
    public void testLastUpdateTime() {
        sessionStats.increaseOutputBytes(FIXED_INCREASE);
        Assert.assertTrue(System.currentTimeMillis() - sessionStats.lastUpdateTime() < 10);
        sleep(100);
        Assert.assertTrue(System.currentTimeMillis() - sessionStats.lastUpdateTime() >= 100);
    }

    @Test
    public void testGetAutoReadEvents() {
        List<SessionStats.AutoReadEvent> events = sessionStats.getAutoReadEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.isEmpty());

        sessionStats.onWritable();
        events = sessionStats.getAutoReadEvents();
        Assert.assertNotNull(events);
        Assert.assertTrue(events.isEmpty());

        sessionStats.onNotWritable();
        sleep(100);
        sessionStats.onWritable();
        Assert.assertNotNull(events);
        Assert.assertFalse(events.isEmpty());

        SessionStats.AutoReadEvent event = events.get(0);
        logger.info("{}", event);
    }
}