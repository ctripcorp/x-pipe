package com.ctrip.xpipe.redis.checker.healthcheck.keeperstate;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KeeperStoreTypeResolverTest {

    private static final String ACTIVE_DC = "jq";
    private static final String BACKUP_DC = "oy";
    private static final String CLUSTER = "cluster";
    private static final String SHARD = "shard";

    @Mock private MetaCache metaCache;
    @Mock private KeeperStateCache keeperStateCache;

    private KeeperStoreTypeResolver resolver;

    // 用例会改写 active 标记，故每个用例新建，避免相互污染
    private KeeperMeta ACTIVE_DC_TFS;
    private KeeperMeta ACTIVE_DC_LOCAL;
    private KeeperMeta BACKUP_DC_TFS;
    private KeeperMeta BACKUP_DC_LOCAL;

    @Before
    public void setUp() {
        resolver = new KeeperStoreTypeResolver(metaCache, keeperStateCache);
        ACTIVE_DC_TFS = keeper("10.0.0.1", 1L);
        ACTIVE_DC_LOCAL = keeper("10.0.0.2", 2L);
        BACKUP_DC_TFS = keeper("10.0.1.1", 3L);
        BACKUP_DC_LOCAL = keeper("10.0.1.2", 4L);
    }

    @Test
    public void testActiveDcInstanceGetsNoTag() {
        Assert.assertNull(resolver.resolve(info(ACTIVE_DC, false)));
        Assert.assertNull(resolver.resolve(null));
        verify(metaCache, never()).getKeeperOfDcClusterShard(anyString(), anyString(), anyString());
    }

    /**
     * 双向同步集群没有 activeDc 也不支持 Keeper；isInActiveDc() 对空 activeDc 返回 false，
     * 若只看它就会把这类实例当成跨机房实例，凭空打出一个 00。
     */
    @Test
    public void testClusterWithoutKeeperChainGetsNoTag() {
        DefaultRedisInstanceInfo biDirection = new DefaultRedisInstanceInfo(BACKUP_DC, CLUSTER, SHARD,
                new HostPort("10.1.0.1", 6379), null, ClusterType.BI_DIRECTION);
        Assert.assertNull(resolver.resolve(biDirection));

        // ONE_WAY 但 activeDc 缺失（meta 不全）同样无从判断
        DefaultRedisInstanceInfo noActiveDc = new DefaultRedisInstanceInfo(BACKUP_DC, CLUSTER, SHARD,
                new HostPort("10.1.0.2", 6379), null, ClusterType.ONE_WAY);
        Assert.assertNull(resolver.resolve(noActiveDc));

        verify(metaCache, never()).getKeeperOfDcClusterShard(anyString(), anyString(), anyString());
    }

    /** 两端都只有本地盘 Keeper：不该去问实时状态。 */
    @Test
    public void testBothLocal() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_LOCAL);
        givenKeepers(BACKUP_DC, BACKUP_DC_LOCAL);

        Assert.assertEquals("00", resolver.resolve(info(BACKUP_DC, false)));
        verify(keeperStateCache, never()).isActive(any());
    }

    /** 两端都是 TFS：无论哪个 Keeper 在用都是 TFS，同样不问实时状态。 */
    @Test
    public void testBothTfs() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_TFS);
        givenKeepers(BACKUP_DC, BACKUP_DC_TFS);

        Assert.assertEquals("11", resolver.resolve(info(BACKUP_DC, false)));
        verify(keeperStateCache, never()).isActive(any());
    }

    @Test
    public void testNoKeeperAtAllReadsAsLocal() {
        when(metaCache.getKeeperOfDcClusterShard(ACTIVE_DC, CLUSTER, SHARD))
                .thenReturn(Collections.emptyList());
        when(metaCache.getKeeperOfDcClusterShard(BACKUP_DC, CLUSTER, SHARD)).thenReturn(null);

        Assert.assertEquals("00", resolver.resolve(info(BACKUP_DC, false)));
    }

    /** 混部时链路走在当下的 ACTIVE Keeper 上，这是唯一需要查缓存的分支。 */
    @Test
    public void testMixedDeploymentFollowsActiveKeeper() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_TFS, ACTIVE_DC_LOCAL);
        givenKeepers(BACKUP_DC, BACKUP_DC_LOCAL);

        when(keeperStateCache.isActive(hostPort(ACTIVE_DC_TFS))).thenReturn(true);
        Assert.assertEquals("10", resolver.resolve(info(BACKUP_DC, false)));

        when(keeperStateCache.isActive(hostPort(ACTIVE_DC_TFS))).thenReturn(false);
        Assert.assertEquals("00", resolver.resolve(info(BACKUP_DC, false)));
    }

    /**
     * 实时状态采不到（Keeper 失联或正在切换，窗口十几秒）时降级用 meta 的 active 标记，
     * 而不是丢掉 tag —— 否则延迟突增那批点恰好失去归因能力。
     */
    @Test
    public void testUnknownStateFallsBackToMetaActive() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_TFS, ACTIVE_DC_LOCAL);
        givenKeepers(BACKUP_DC, BACKUP_DC_LOCAL);
        when(keeperStateCache.isActive(hostPort(ACTIVE_DC_TFS))).thenReturn(null);

        ACTIVE_DC_TFS.setActive(true);
        Assert.assertEquals("10", resolver.resolve(info(BACKUP_DC, false)));

        // meta 说 TFS 不是 active（如故障后 meta-server 已选出 BM keeper）→ 收敛到本地盘
        ACTIVE_DC_TFS.setActive(false);
        Assert.assertEquals("00", resolver.resolve(info(BACKUP_DC, false)));
    }

    /** 实时状态可用时不看 meta —— meta 的 active 标记滞后于实际切换。 */
    @Test
    public void testLiveStateWinsOverMeta() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_TFS, ACTIVE_DC_LOCAL);
        givenKeepers(BACKUP_DC, BACKUP_DC_LOCAL);

        ACTIVE_DC_TFS.setActive(false);
        when(keeperStateCache.isActive(hostPort(ACTIVE_DC_TFS))).thenReturn(true);
        Assert.assertEquals("10", resolver.resolve(info(BACKUP_DC, false)));

        ACTIVE_DC_TFS.setActive(true);
        when(keeperStateCache.isActive(hostPort(ACTIVE_DC_TFS))).thenReturn(false);
        Assert.assertEquals("00", resolver.resolve(info(BACKUP_DC, false)));
    }

    @Test
    public void testBackupDcMixedDeploymentFollowsActiveKeeper() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_LOCAL);
        givenKeepers(BACKUP_DC, BACKUP_DC_TFS, BACKUP_DC_LOCAL);

        when(keeperStateCache.isActive(hostPort(BACKUP_DC_TFS))).thenReturn(true);
        Assert.assertEquals("01", resolver.resolve(info(BACKUP_DC, false)));
    }

    /**
     * 跨 region 的 backup dc 不部署 TFS，第二位恒为本地盘；该机房的 Keeper 也不在本 checker
     * 的采集范围内，所以不能去查它的状态。
     */
    @Test
    public void testCrossRegionBackupDcIsAlwaysLocal() {
        givenKeepers(ACTIVE_DC, ACTIVE_DC_TFS);

        Assert.assertEquals("10", resolver.resolve(info(BACKUP_DC, true)));
        verify(metaCache, never()).getKeeperOfDcClusterShard(eq(BACKUP_DC), anyString(), anyString());
        verify(keeperStateCache, never()).isActive(any());
    }

    private void givenKeepers(String dcId, KeeperMeta... keepers) {
        when(metaCache.getKeeperOfDcClusterShard(dcId, CLUSTER, SHARD)).thenReturn(Arrays.asList(keepers));
        for (KeeperMeta keeper : keepers) {
            boolean tfs = keeper == ACTIVE_DC_TFS || keeper == BACKUP_DC_TFS;
            when(metaCache.getKeeperDiskType(dcId, keeper)).thenReturn(tfs ? "tfs" : "local");
        }
    }

    private DefaultRedisInstanceInfo info(String dcId, boolean crossRegion) {
        DefaultRedisInstanceInfo info = new DefaultRedisInstanceInfo(dcId, CLUSTER, SHARD,
                new HostPort("10.1.0.1", 6379), ACTIVE_DC, ClusterType.ONE_WAY);
        info.setCrossRegion(crossRegion);
        return info;
    }

    private static HostPort hostPort(KeeperMeta keeper) {
        return new HostPort(keeper.getIp(), keeper.getPort());
    }

    private static KeeperMeta keeper(String ip, Long containerId) {
        KeeperMeta keeper = new KeeperMeta();
        keeper.setIp(ip);
        keeper.setPort(6380);
        keeper.setKeeperContainerId(containerId);
        return keeper;
    }
}
