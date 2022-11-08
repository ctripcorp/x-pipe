package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author ayq
 * <p>
 * 2022/8/26 16:32
 */
public class DefaultRedisGtidCollectorTest {

    private DefaultRedisGtidCollector defaultRedisGtidCollector;

    @Before
    public void setUp() throws Exception {
        defaultRedisGtidCollector = new DefaultRedisGtidCollector(1L,1L, Mockito.mock(DcMetaCache.class),
                Mockito.mock(CurrentMetaManager.class), Mockito.mock(MultiDcService.class),
                Mockito.mock(ScheduledExecutorService.class), Mockito.mock(XpipeNettyClientKeyedObjectPool.class),
                DefaultRedisGtidCollector.REDIS_INFO_GTID_INTERVAL_SECONDS_DR_MASTER_GROUP);
    }

    @Test
    public void testSrcChanged() {
        assertFalse(defaultRedisGtidCollector.sidsChanged("", null));
        assertFalse(defaultRedisGtidCollector.sidsChanged("", "a,b"));
        assertFalse(defaultRedisGtidCollector.sidsChanged("a,b", "a,b"));
        assertFalse(defaultRedisGtidCollector.sidsChanged("a,b", "b,a"));

        assertTrue(defaultRedisGtidCollector.sidsChanged("a,b", ""));
        assertTrue(defaultRedisGtidCollector.sidsChanged("a,b,c", "b,a"));
        assertTrue(defaultRedisGtidCollector.sidsChanged("a,b", "a,b,c"));
    }

}