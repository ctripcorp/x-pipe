package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


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
        sessionStats = new DefaultSessionStats(scheduled);
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
}