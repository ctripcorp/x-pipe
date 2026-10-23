package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperStateCacheTest {

    private static final HostPort KEEPER = new HostPort("10.0.0.1", 6380);

    @Mock private CheckerConfig checkerConfig;

    private KeeperStateCache cache;

    @Before
    public void setUp() {
        when(checkerConfig.getRedisReplicationHealthCheckInterval()).thenReturn(2000);
        cache = new KeeperStateCache(checkerConfig);
    }

    @Test
    public void testUnknownUntilSampled() {
        Assert.assertNull(cache.isActive(KEEPER));
        Assert.assertNull(cache.isActive(null));

        cache.update(KEEPER, KeeperState.ACTIVE);
        Assert.assertEquals(Boolean.TRUE, cache.isActive(KEEPER));

        cache.update(KEEPER, KeeperState.BACKUP);
        Assert.assertEquals(Boolean.FALSE, cache.isActive(KEEPER));
    }

    /** PREPARE 等过渡态既非 active 也非 backup，但已采到，不能当作未知。 */
    @Test
    public void testTransitionalStateIsNotActive() {
        cache.update(KEEPER, KeeperState.PREPARE);
        Assert.assertEquals(Boolean.FALSE, cache.isActive(KEEPER));
    }

    @Test
    public void testIgnoreNullInput() {
        cache.update(null, KeeperState.ACTIVE);
        cache.update(KEEPER, null);
        Assert.assertNull(cache.isActive(KEEPER));
        cache.remove(null);
    }

    @Test
    public void testRemoveDropsSample() {
        cache.update(KEEPER, KeeperState.ACTIVE);
        cache.remove(KEEPER);
        Assert.assertNull(cache.isActive(KEEPER));
    }

    /** 周期配成 0 时若直接用它算过期窗口，样本会立刻失效，该 tag 永远打不出来。 */
    @Test
    public void testZeroIntervalStillKeepsFreshSample() {
        when(checkerConfig.getRedisReplicationHealthCheckInterval()).thenReturn(0);

        cache.update(KEEPER, KeeperState.ACTIVE);
        Assert.assertEquals(Boolean.TRUE, cache.isActive(KEEPER));
    }

    /** 过期样本必须与「本地盘」区分：Keeper 失联后不应继续按陈旧 ACTIVE 判断。 */
    @SuppressWarnings("unchecked")
    @Test
    public void testExpiredSampleReadsAsUnknown() {
        cache.update(KEEPER, KeeperState.ACTIVE);
        Map<HostPort, Object> samples =
                (Map<HostPort, Object>) ReflectionTestUtils.getField(cache, "samples");
        Assert.assertNotNull(samples);
        Object sample = samples.get(KEEPER);
        ReflectionTestUtils.setField(sample, "recvTimeMilli",
                System.currentTimeMillis() - 2 * 2000, long.class);

        Assert.assertNull(cache.isActive(KEEPER));

        cache.update(KEEPER, KeeperState.ACTIVE);
        Assert.assertEquals(Boolean.TRUE, cache.isActive(KEEPER));
    }
}
