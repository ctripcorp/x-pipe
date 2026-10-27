package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperdelay.KeeperDelayActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.keeperstate.KeeperStateActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultKeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.meta.KeeperCheckSelector;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperStateCacheTest {

    private static final HostPort KEEPER = new HostPort("10.0.0.1", 6380);

    private static final String DC = "jq";

    @Mock private CheckerConfig checkerConfig;

    @Mock private MetaCache metaCache;

    private KeeperStateCache cache;

    @Before
    public void setUp() {
        when(checkerConfig.getKeeperStateCheckIntervalMilli()).thenReturn(2000);
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
        Assert.assertTrue(samples().isEmpty());
    }

    /** 周期配成 0 时若直接用它算过期窗口，样本会立刻失效，该 tag 永远打不出来。 */
    @Test
    public void testZeroIntervalStillKeepsFreshSample() {
        when(checkerConfig.getKeeperStateCheckIntervalMilli()).thenReturn(0);

        cache.update(KEEPER, KeeperState.ACTIVE);
        Assert.assertEquals(Boolean.TRUE, cache.isActive(KEEPER));
    }

    @Test
    public void testGetStateReturnsRawState() {
        Assert.assertNull(cache.getState(KEEPER));
        Assert.assertNull(cache.getState(null));

        cache.update(KEEPER, KeeperState.PRE_ACTIVE);
        Assert.assertEquals(KeeperState.PRE_ACTIVE, cache.getState(KEEPER));

        expire(KEEPER);
        Assert.assertNull("过期样本按未采到处理", cache.getState(KEEPER));
    }

    /** 不在检查范围内的 Keeper 与过期样本被定时清理；仍在用且新鲜的保留。 */
    @Test
    public void testRemoveUnusedKeepersDropsUnusedAndExpired() {
        HostPort removedKeeper = new HostPort("10.0.0.2", 6380);
        HostPort staleKeeper = new HostPort("10.0.0.3", 6380);
        useKeepers(KEEPER, staleKeeper);

        cache.update(KEEPER, KeeperState.ACTIVE);
        cache.update(removedKeeper, KeeperState.BACKUP);
        cache.update(staleKeeper, KeeperState.BACKUP);
        expire(staleKeeper);

        cache.removeUnusedKeepers();

        Assert.assertEquals(Collections.singleton(KEEPER), samples().keySet());
        Assert.assertEquals(Boolean.TRUE, cache.isActive(KEEPER));
    }

    /** meta 不可用时无法判定在用集合，只按过期清理，不得清空全部。 */
    @Test
    public void testRemoveUnusedKeepersKeepsFreshWhenMetaUnavailable() {
        cache.setMetaCache(metaCache).setKeeperSelector(new KeeperCheckSelector(metaCache, DC));
        when(metaCache.getXpipeMeta()).thenReturn(null);
        HostPort staleKeeper = new HostPort("10.0.0.3", 6380);
        cache.update(KEEPER, KeeperState.ACTIVE);
        cache.update(staleKeeper, KeeperState.BACKUP);
        expire(staleKeeper);

        cache.removeUnusedKeepers();

        Assert.assertEquals(Collections.singleton(KEEPER), samples().keySet());
    }

    /** meta 明确不含任何 Keeper 时全部回收，口径同 DefaultKeeperSessionManager。 */
    @Test
    public void testRemoveUnusedKeepersClearsAllWhenNoKeeperInUse() {
        useKeepers();
        cache.update(KEEPER, KeeperState.ACTIVE);

        cache.removeUnusedKeepers();

        Assert.assertTrue(samples().isEmpty());
    }

    /** 判定之后写入的新样本不能被一起删掉：清理用的是条件删除。 */
    @Test
    public void testRemoveUnusedKeepersDoesNotDropConcurrentlyRefreshedSample() {
        useKeepers(KEEPER);
        cache.update(KEEPER, KeeperState.ACTIVE);
        Object stale = samples().get(KEEPER);
        // 时间严格更新，确保新样本替换旧样本而非被 merge 视作同一时刻
        cache.onAction(success(KeeperState.BACKUP, System.currentTimeMillis() + 1));

        Assert.assertFalse("旧样本已被替换，条件删除不得命中", samples().remove(KEEPER, stale));
        cache.removeUnusedKeepers();
        Assert.assertEquals(KeeperState.BACKUP, cache.getState(KEEPER));
    }

    /** 作为 listener：成功写入角色，失败退回未知。 */
    @Test
    public void testOnActionSuccessAndFailure() {
        long now = System.currentTimeMillis();
        cache.onAction(success(KeeperState.ACTIVE, now));
        Assert.assertEquals(KeeperState.ACTIVE, cache.getState(KEEPER));

        cache.onAction(failure(now + 1));
        Assert.assertNull("问不通就不再声称知道角色", cache.getState(KEEPER));
        Assert.assertNull(cache.isActive(KEEPER));
    }

    /**
     * listener 异步回调，到达顺序不保证：早发出的请求晚到达时不得覆盖晚发出的结果，
     * 无论早到的是成功还是失败。
     */
    @Test
    public void testOnActionKeepsNewerRequestRegardlessOfArrivalOrder() {
        long now = System.currentTimeMillis();

        cache.onAction(success(KeeperState.ACTIVE, now));
        cache.onAction(failure(now - 1000));
        Assert.assertEquals("早发出的失败晚到，不得抹掉新结果", KeeperState.ACTIVE, cache.getState(KEEPER));

        cache.onAction(failure(now + 1000));
        cache.onAction(success(KeeperState.ACTIVE, now + 500));
        Assert.assertNull("早发出的成功晚到，不得复活已失败的角色", cache.getState(KEEPER));
    }

    /** action 停止时 stopWatch 删掉该 Keeper 的条目。 */
    @Test
    public void testStopWatchRemovesEntry() {
        cache.update(KEEPER, KeeperState.ACTIVE);
        @SuppressWarnings("unchecked")
        HealthCheckAction<KeeperHealthCheckInstance> action = mock(HealthCheckAction.class);
        when(action.getActionInstance()).thenReturn(instance());

        cache.stopWatch(action);

        Assert.assertTrue(samples().isEmpty());
    }

    @Test
    public void testListenerContract() {
        Assert.assertTrue(cache.supportInstance(instance()));
        Assert.assertTrue(cache.worksfor(success(KeeperState.ACTIVE, System.currentTimeMillis())));
        Assert.assertFalse(cache.worksfor(new KeeperDelayActionContext(instance(), 1L)));
    }

    /** 过期样本必须与「本地盘」区分：Keeper 失联后不应继续按陈旧 ACTIVE 判断。 */
    @Test
    public void testExpiredSampleReadsAsUnknown() {
        cache.update(KEEPER, KeeperState.ACTIVE);
        expire(KEEPER);

        Assert.assertNull(cache.isActive(KEEPER));

        cache.update(KEEPER, KeeperState.ACTIVE);
        Assert.assertEquals(Boolean.TRUE, cache.isActive(KEEPER));
    }

    @SuppressWarnings("unchecked")
    private Map<HostPort, Object> samples() {
        Map<HostPort, Object> samples = (Map<HostPort, Object>) ReflectionTestUtils.getField(cache, "samples");
        Assert.assertNotNull(samples);
        return samples;
    }

    /** 样本时间回拨两个周期（2 × 2000ms），恰好达到过期阈值。 */
    private void expire(HostPort keeper) {
        ReflectionTestUtils.setField(samples().get(keeper), "timeMilli",
                System.currentTimeMillis() - 2 * 2000, long.class);
    }

    private KeeperStateActionContext success(KeeperState state, long requestTimeMilli) {
        return new KeeperStateActionContext(instance(), state, requestTimeMilli);
    }

    private KeeperStateActionContext failure(long requestTimeMilli) {
        return new KeeperStateActionContext(instance(), new RuntimeException("timeout"), requestTimeMilli);
    }

    /** 构造只含给定 Keeper 的 meta：ONE_WAY、ActiveDc 即本 Checker 所在机房，全部会被 selector 选中。 */
    private void useKeepers(HostPort... keepers) {
        ShardMeta shard = new ShardMeta().setId("shard").setDbId(1L);
        for (HostPort keeper : keepers) {
            shard.addKeeper(new KeeperMeta().setIp(keeper.getHost()).setPort(keeper.getPort()));
        }
        ClusterMeta cluster = new ClusterMeta().setId("cluster").setType(ClusterType.ONE_WAY.toString())
                .setActiveDc(DC);
        cluster.addShard(shard);
        DcMeta dcMeta = new DcMeta(DC);
        dcMeta.addCluster(cluster);
        when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta().addDc(dcMeta));
        cache.setMetaCache(metaCache).setKeeperSelector(new KeeperCheckSelector(metaCache, DC));
    }

    private DefaultKeeperHealthCheckInstance instance() {
        DefaultKeeperHealthCheckInstance instance = new DefaultKeeperHealthCheckInstance();
        instance.setInstanceInfo(new DefaultKeeperInstanceInfo("jq", "cluster", "shard", 1L, KEEPER, "jq",
                ClusterType.ONE_WAY));
        return instance;
    }
}
