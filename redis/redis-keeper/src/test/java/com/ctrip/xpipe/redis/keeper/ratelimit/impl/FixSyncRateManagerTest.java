package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author lishanglin
 * date 2024/9/14
 */
@RunWith(MockitoJUnitRunner.class)
public class FixSyncRateManagerTest {

    @Mock
    private KeeperConfig keeperConfig;

    private FixSyncRateManager fixSyncRateManager;

    @Before
    public void setupFixSyncRateManagerTest() {
        this.fixSyncRateManager = new FixSyncRateManager(keeperConfig);
        Mockito.when(keeperConfig.fsyncRateLimit()).thenReturn(true);
    }

    @Test
    public void testSyncTrafficRecord() {
        SyncRateLimiter fsyncRateLimiter = fixSyncRateManager.generateFsyncRateLimiter();
        SyncRateLimiter psyncRateLimiter = fixSyncRateManager.generatePsyncRateLimiter();

        fixSyncRateManager.setTotalIOLimit(1000);

        // check no exception for acquire 0
        psyncRateLimiter.acquire(0);
        fsyncRateLimiter.acquire(0);
        psyncRateLimiter.acquire(100);
        fsyncRateLimiter.acquire(100);
    }

}
