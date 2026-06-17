package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;

@RunWith(MockitoJUnitRunner.class)
public class DefaultBeaconManagerTest extends AbstractConsoleTest {

    private static final String CLUSTER_ID = "cluster1";
    private static final int ORG_ID = 1;
    private static final ClusterType CLUSTER_TYPE = ClusterType.ONE_WAY;
    private static final String LAST_MODIFY_TIME = "20200101103030001";

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private MetaCache metaCache;

    @Mock
    private MonitorService monitorService;

    private DefaultBeaconManager beaconManager;

    private Set<MonitorGroupMeta> groups;
    private int localHash;

    @Before
    public void setUp() {
        beaconManager = new DefaultBeaconManager(monitorManager, beaconMetaService, checkerConfig, metaCache);

        groups = Collections.singleton(new MonitorGroupMeta("shard1", "jq",
                Collections.singleton(new HostPort("127.0.0.1", 6379)), true));
        localHash = new MonitorClusterMeta(groups, hashExtra(LAST_MODIFY_TIME)).generateHashCodeForBeaconCheck();
    }

    @Test
    public void computeClusterMetaHash_shouldMatchCheckClusterHashLocalSide_forDrRoute() {
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.DR, CLUSTER_TYPE);
    }

    @Test
    public void computeClusterMetaHash_shouldMatchCheckClusterHashLocalSide_forSentinelRoute() {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", "jq",
                        Collections.singleton(new HostPort("127.0.0.1", 6379)), true)
        )));
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconShards(CLUSTER_ID, "jq", Collections.emptyMap())).thenReturn(shards);

        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, CLUSTER_TYPE);
    }

    @Test
    public void computeClusterMetaHash_shouldUseMetaCacheLastModifyTimeSameAsCheckClusterHashParam() {
        mockDrMeta();
        mockClusterLastModifyTime(LAST_MODIFY_TIME);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashFromCheckParam = new MonitorClusterMeta(groups, hashExtra(LAST_MODIFY_TIME))
                .generateHashCodeForBeaconCheck();

        Assert.assertEquals(hashFromCheckParam, hashFromCompute);
    }

    @Test
    public void computeClusterMetaHash_shouldDifferFromCheckClusterHash_whenStaleInstanceLastModifyTime() {
        mockDrMeta();
        String staleLastModifyTime = "20200101103030001";
        String freshLastModifyTime = "20200101103030002";
        mockClusterLastModifyTime(freshLastModifyTime);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashFromCheckStaleParam = new MonitorClusterMeta(groups, hashExtra(staleLastModifyTime))
                .generateHashCodeForBeaconCheck();

        Assert.assertNotEquals("stale ClusterInstanceInfo lastModifyTime should produce different local hash",
                hashFromCheckStaleParam, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, staleLastModifyTime);
        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void clusterConsistentWithLastModifyTimeExtra_shouldReturnConsistency() {
        mockDrMeta();
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.DR, CLUSTER_TYPE);
    }

    @Test
    public void clusterChanged_shouldReturnInconsistency() {
        mockDrMeta();
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(true);
        Map<String, String> clusterExtra = new HashMap<>();
        clusterExtra.put("lastModifyTime", "20200101103030000");
        Mockito.when(monitorService.getBeaconClusterExtra("xpipe", CLUSTER_ID)).thenReturn(clusterExtra);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void clusterChanged_butBeaconLastModifyTimeNewer_shouldReturnInconsistencyIgnore() {
        mockDrMeta();
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(true);
        Map<String, String> clusterExtra = new HashMap<>();
        clusterExtra.put("lastModifyTime", "20200101103030002");
        Mockito.when(monitorService.getBeaconClusterExtra("xpipe", CLUSTER_ID)).thenReturn(clusterExtra);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY_IGNORE, status);
    }

    @Test
    public void checkBeaconLastModifyTimeDisabled_shouldSkipExtraCheckAndReturnInconsistency() {
        mockDrMeta();
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(false);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
        Mockito.verify(monitorService, Mockito.never()).getBeaconClusterExtra("xpipe", CLUSTER_ID);
    }

    @Test
    public void getBeaconClusterExtraThrow404_shouldSkipLastModifyTimeCheckAndReturnInconsistency() {
        mockDrMeta();
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(true);
        Mockito.when(monitorService.getBeaconClusterExtra("xpipe", CLUSTER_ID))
                .thenThrow(new BadRequestException("404 NotFound"));

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void sentinelRouteShouldUseShardsForHashCheck() {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", "jq",
                        Collections.singleton(new HostPort("127.0.0.1", 6379)), true)
        )));
        int shardHash = new MonitorClusterMeta(null, shards, hashExtra(LAST_MODIFY_TIME)).generateHashCodeForBeaconCheck();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconShards(CLUSTER_ID, "jq", Collections.emptyMap())).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, CLUSTER_TYPE);
    }

    @Test
    public void sentinelRouteSingleDcShouldUseOneWaySystem() {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", "jq",
                        Collections.singleton(new HostPort("127.0.0.1", 6379)), true)
        )));
        int shardHash = new MonitorClusterMeta(null, shards, hashExtra(LAST_MODIFY_TIME)).generateHashCodeForBeaconCheck();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconShards(CLUSTER_ID, "jq", Collections.emptyMap())).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, ClusterType.SINGLE_DC, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, ClusterType.SINGLE_DC);
    }

    @Test
    public void sentinelRegisterShouldBuildShardsWithPublishMasters() {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6380", "jq",
                        Collections.singleton(new HostPort("127.0.0.1", 6380)), true)
        )));
        Map<String, HostPort> shardMasters = Collections.singletonMap("shard1", new HostPort("127.0.0.1", 6380));
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconShards(CLUSTER_ID, "jq", shardMasters)).thenReturn(shards);

        beaconManager.registerCluster(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.SENTINEL, shardMasters);

        Mockito.verify(monitorService).registerCluster(Mockito.eq("xpipe"), Mockito.eq(CLUSTER_ID), Mockito.isNull(),
                Mockito.eq(shards), Mockito.anyMap());
    }

    private void mockDrMeta() {
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.DR)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconGroups(CLUSTER_ID)).thenReturn(groups);
    }

    private void mockClusterLastModifyTime(String lastModifyTime) {
        ClusterMeta clusterMeta = new ClusterMeta(CLUSTER_ID);
        clusterMeta.setLastModifiedTime(lastModifyTime);
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta("jq").addCluster(clusterMeta));
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    private Map<String, String> hashExtra(String lastModifyTime) {
        return Collections.singletonMap("lastModifyTime", lastModifyTime);
    }

    /**
     * checkClusterHash 与 computeClusterMetaHash 应使用同一 local hash：
     * 当 Beacon 返回 computeClusterMetaHash 的结果时，checkClusterHash 应判定为 CONSISTENCY。
     */
    private void assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType routeType, ClusterType clusterType) {
        if (routeType == BeaconRouteType.DR) {
            mockDrMeta();
        }
        mockClusterLastModifyTime(LAST_MODIFY_TIME);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, clusterType, routeType);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);

        Assert.assertEquals(hashFromCompute,
                beaconManager.computeClusterMetaHash(CLUSTER_ID, clusterType, routeType));
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, clusterType, ORG_ID, LAST_MODIFY_TIME, routeType));
    }
}
