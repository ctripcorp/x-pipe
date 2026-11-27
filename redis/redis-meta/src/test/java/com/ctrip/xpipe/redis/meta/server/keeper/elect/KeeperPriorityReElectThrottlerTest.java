package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** tick() is driven by hand; the scheduler is not started. */
@RunWith(MockitoJUnitRunner.class)
public class KeeperPriorityReElectThrottlerTest {

    @Mock
    private KeeperElectReElectService reElectService;

    private UnitTestServerConfig config;

    private KeeperPriorityReElectThrottler throttler;

    @Before
    public void setUp() {
        config = new UnitTestServerConfig();
        throttler = new KeeperPriorityReElectThrottler();
        throttler.setMetaServerConfig(config);
        throttler.setKeeperElectReElectService(reElectService);
    }

    @Test
    public void testRateZeroReElectsImmediately() {
        config.setKeeperPriorityReElectRatePerSecond(0);

        throttler.submit(1L, 10L);

        verify(reElectService).reElect(1L, 10L);
        Assert.assertEquals(0, throttler.pendingSize());
    }

    @Test
    public void testRateLimitsPerTick() {
        // 5/s with 200ms ticks → 1 shard per tick
        config.setKeeperPriorityReElectRatePerSecond(5);
        for (long shard = 1; shard <= 5; shard++) {
            throttler.submit(1L, shard);
        }
        verify(reElectService, never()).reElect(anyLong(), anyLong());

        throttler.tick();
        verify(reElectService, times(1)).reElect(anyLong(), anyLong());
        Assert.assertEquals(4, throttler.pendingSize());

        for (int i = 0; i < 4; i++) {
            throttler.tick();
        }
        verify(reElectService, times(5)).reElect(anyLong(), anyLong());
        Assert.assertEquals(0, throttler.pendingSize());
    }

    @Test
    public void testFifoOrder() {
        config.setKeeperPriorityReElectRatePerSecond(1000);
        throttler.submit(1L, 3L);
        throttler.submit(1L, 1L);
        throttler.submit(2L, 2L);

        throttler.tick();

        InOrder order = inOrder(reElectService);
        order.verify(reElectService).reElect(1L, 3L);
        order.verify(reElectService).reElect(1L, 1L);
        order.verify(reElectService).reElect(2L, 2L);
    }

    @Test
    public void testLowRateCarriesFractionalPermits() {
        // 1/s → 0.2 permit per tick → one shard every 5 ticks
        config.setKeeperPriorityReElectRatePerSecond(1);
        throttler.submit(1L, 1L);
        throttler.submit(1L, 2L);

        for (int i = 0; i < 4; i++) {
            throttler.tick();
        }
        verify(reElectService, never()).reElect(anyLong(), anyLong());
        throttler.tick();
        verify(reElectService, times(1)).reElect(anyLong(), anyLong());
    }

    @Test
    public void testDuplicateShardReElectedOnce() {
        config.setKeeperPriorityReElectRatePerSecond(100);
        throttler.submit(1L, 1L);
        throttler.submit(1L, 1L);
        Assert.assertEquals(1, throttler.pendingSize());

        throttler.tick();

        verify(reElectService, times(1)).reElect(1L, 1L);
    }

    @Test
    public void testFailureDoesNotStopQueue() {
        config.setKeeperPriorityReElectRatePerSecond(100);
        doThrow(new IllegalStateException("cluster moved")).when(reElectService).reElect(1L, 1L);
        throttler.submit(1L, 1L);
        throttler.submit(1L, 2L);

        throttler.tick();

        verify(reElectService).reElect(1L, 2L);
        Assert.assertEquals(0, throttler.pendingSize());
    }

    @Test
    public void testDisablingRateDrainsQueue() {
        config.setKeeperPriorityReElectRatePerSecond(1);
        for (long shard = 1; shard <= 10; shard++) {
            throttler.submit(1L, shard);
        }

        config.setKeeperPriorityReElectRatePerSecond(0);
        throttler.tick();

        verify(reElectService, times(10)).reElect(anyLong(), anyLong());
    }
}
