package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.HealthState;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StrategyAwareKeeperActiveElectAlgorithmTest {

    @Mock
    private DcMetaCache dcMetaCache;

    private KeeperMeta bmKeeper;
    private KeeperMeta tfsKeeper;

    @Before
    public void setUp() {
        bmKeeper = keeper(6000, 1L, 1);
        tfsKeeper = keeper(6001, 2L, 2);
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
            KeeperMeta keeperMeta = invocation.getArgument(0);
            KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
            keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
            if (keeperMeta.getKeeperContainerId() == 2L || keeperMeta.getKeeperContainerId() == 3L) {
                keeperContainerMeta.setDiskType("tfs-1");
            } else {
                keeperContainerMeta.setDiskType("DEFAULT");
            }
            return keeperContainerMeta;
        });
    }

    @Test
    public void testAutoSelectHigherPriority() {
        KeeperMeta lowPriority = keeper(6002, 1L, 1);
        KeeperMeta highPriority = keeper(6003, 2L, 5);

        Assert.assertEquals(highPriority,
                auto().select(1L, 1L, Arrays.asList(lowPriority, highPriority)));
    }

    @Test
    public void testAutoSkipsPriorityZero() {
        KeeperMeta zeroPriority = keeper(6002, 1L, 0);
        KeeperMeta valid = keeper(6003, 2L, 1);

        Assert.assertEquals(valid, auto().select(1L, 1L, Arrays.asList(zeroPriority, valid)));
    }

    @Test
    public void testBmPreferSelectsBmWhenAvailable() {
        tfsKeeper.setPriority(10);
        bmKeeper.setPriority(1);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.BM_PREFER, dcMetaCache);

        Assert.assertEquals(bmKeeper, algorithm.select(1L, 1L, Arrays.asList(tfsKeeper, bmKeeper)));
    }

    @Test
    public void testBmPreferFallsBackToAutoWhenNoBm() {
        tfsKeeper.setPriority(1);
        KeeperMeta anotherTfs = keeper(6002, 3L, 5);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.BM_PREFER, dcMetaCache);

        Assert.assertEquals(anotherTfs, algorithm.select(1L, 1L, Arrays.asList(tfsKeeper, anotherTfs)));
    }

    @Test
    public void testSamePriorityKeepsZkOrder() {
        KeeperMeta first = keeper(6002, 1L, 2);
        KeeperMeta second = keeper(6003, 2L, 2);

        Assert.assertEquals(first, auto().select(1L, 1L, Arrays.asList(first, second)));
    }

    @Test
    public void testEmptyListReturnsNull() {
        Assert.assertNull(auto().select(1L, 1L, Collections.emptyList()));
    }

    @Test
    public void testNoElectableKeeperFallsBackToFirstByZkOrder() {
        KeeperMeta zeroPriority = keeper(6002, 1L, 0);

        Assert.assertEquals(zeroPriority,
                auto().select(1L, 1L, Collections.singletonList(zeroPriority)));
    }

    @Test
    public void testNullPriorityNotElectableWithoutNormalizeFallsBackToFirst() {
        KeeperMeta nullPriority = keeper(6002, 1L, null);
        KeeperMeta explicitZero = keeper(6003, 2L, 0);

        Assert.assertEquals(nullPriority,
                auto().select(1L, 1L, Arrays.asList(nullPriority, explicitZero)));
    }

    @Test
    public void testAllExplicitZeroFallsBackToFirstByZkOrder() {
        KeeperMeta zero1 = keeper(6002, 1L, 0);
        KeeperMeta zero2 = keeper(6003, 2L, 0);

        Assert.assertEquals(zero1, auto().select(1L, 1L, Arrays.asList(zero1, zero2)));
    }

    @Test
    public void testAllBmIgnoresHealthState() {
        KeeperMeta unhealthyFirst = keeper(6100, 1L, 2);
        unhealthyFirst.setHealthState(HealthState.DOWN.name());
        KeeperMeta healthySecond = keeper(6101, 4L, 1);

        Assert.assertEquals(unhealthyFirst,
                auto().select(1L, 1L, Arrays.asList(unhealthyFirst, healthySecond)));
    }

    @Test
    public void testAllTfsSkipsUnhealthyKeeper() {
        KeeperMeta unhealthy = keeper(6200, 2L, 10);
        unhealthy.setHealthState(HealthState.DOWN.name());
        KeeperMeta healthy = keeper(6201, 3L, 5);

        Assert.assertEquals(healthy, auto().select(1L, 1L, Arrays.asList(unhealthy, healthy)));
    }

    @Test
    public void testAllTfsUnhealthyElectsByPriority() {
        KeeperMeta down = keeper(6200, 2L, 5);
        down.setHealthState(HealthState.DOWN.name());
        KeeperMeta recovering = keeper(6201, 3L, 10);
        recovering.setHealthState(HealthState.RECOVERED_PENDING.name());

        Assert.assertEquals(recovering, auto().select(1L, 1L, Arrays.asList(down, recovering)));
    }

    @Test
    public void testMixedTfsDownSelectsBm() {
        TfsShard shard = new TfsShard();
        shard.a.setHealthState(HealthState.DOWN.name());

        Assert.assertEquals(shard.b,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b)));
    }

    @Test
    public void testMixedNonActiveTfsDownSelectsBm() {
        TfsShard shard = new TfsShard();
        shard.c.setHealthState(HealthState.DOWN.name());

        Assert.assertEquals(shard.b,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b)));
    }

    @Test
    public void testMixedTfsRecoveredPendingSelectsBm() {
        TfsShard shard = new TfsShard();
        shard.a.setHealthState(HealthState.RECOVERED_PENDING.name());

        Assert.assertEquals(shard.b,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b)));
    }

    @Test
    public void testMixedUnhealthyTfsSelectsBmEvenIfBmUnhealthy() {
        TfsShard shard = new TfsShard();
        shard.a.setHealthState(HealthState.DOWN.name());
        shard.b.setHealthState(HealthState.DOWN.name());

        Assert.assertEquals(shard.b,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b)));
    }

    @Test
    public void testMixedHealthyTfsSelectsHigherPriorityTfs() {
        TfsShard shard = new TfsShard();

        Assert.assertEquals(shard.c,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.c, shard.a, shard.b)));
    }

    @Test
    public void testMixedMissingTfsDoesNotCountAsUnhealthy() {
        TfsShard shard = new TfsShard();

        Assert.assertEquals(shard.c,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.c, shard.b)));
    }

    @Test
    public void testMixedBmPreferSelectsBmWhenAllTfsHealthy() {
        TfsShard shard = new TfsShard();
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.BM_PREFER, dcMetaCache);

        Assert.assertEquals(shard.b,
                algorithm.select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b)));
    }

    @Test
    public void testAutoSurvivesKeeperWhoseContainerLeftDcMeta() {
        TfsShard shard = new TfsShard();
        KeeperMeta stale = staleKeeper();

        Assert.assertEquals(shard.a,
                shard.algorithm().select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b, stale)));
    }

    @Test
    public void testBmPreferSurvivesKeeperWhoseContainerLeftDcMeta() {
        TfsShard shard = new TfsShard();
        KeeperMeta stale = staleKeeper();
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.BM_PREFER, dcMetaCache);

        Assert.assertEquals(shard.b,
                algorithm.select(1L, 1L, Arrays.asList(shard.a, shard.c, shard.b, stale)));
    }

    private KeeperMeta staleKeeper() {
        KeeperMeta stale = keeper(7003, 99L, KeeperPriorityUtils.DEFAULT_PRIORITY);
        doThrow(new IllegalArgumentException("[getKeeperContainer][unfound keepercontainer]"))
                .when(dcMetaCache).getKeeperContainer(
                        argThat(k -> k != null && Long.valueOf(99L).equals(k.getKeeperContainerId())));
        return stale;
    }

    private StrategyAwareKeeperActiveElectAlgorithm auto() {
        return new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.AUTO, dcMetaCache);
    }

    private class TfsShard {
        final KeeperMeta a = keeper(7000, 2L, 10);
        final KeeperMeta c = keeper(7001, 3L, 10);
        final KeeperMeta b = keeper(7002, 1L, 1);

        StrategyAwareKeeperActiveElectAlgorithm algorithm() {
            return auto();
        }
    }

    private KeeperMeta keeper(int port, long keeperContainerId, Integer priority) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp("127.0.0.1");
        keeperMeta.setPort(port);
        keeperMeta.setKeeperContainerId(keeperContainerId);
        keeperMeta.setPriority(priority);
        return keeperMeta;
    }
}
