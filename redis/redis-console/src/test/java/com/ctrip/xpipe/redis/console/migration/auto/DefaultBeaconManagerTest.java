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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class DefaultBeaconManagerTest extends AbstractConsoleTest {

    private static final String CLUSTER_ID = "cluster1";
    private static final int ORG_ID = 1;
    private static final ClusterType CLUSTER_TYPE = ClusterType.ONE_WAY;
    private static final String LAST_MODIFY_TIME = "20200101103030001";
    private static final String DC = "jq";
    private static final String ZONE = "SHA";

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

    @Before
    public void setUp() {
        beaconManager = new DefaultBeaconManager(monitorManager, beaconMetaService, checkerConfig, metaCache);

        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta(DC).setZone(ZONE));
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        groups = Collections.singleton(new MonitorGroupMeta("shard1", DC,
                Collections.singleton(new HostPort("127.0.0.1", 6379)), true));
    }

    @Test
    public void shouldComputeExtraInHash_true_shouldIncludeExtraInLocalHash() {
        mockDrMeta();
        mockClusterLastModifyTime(LAST_MODIFY_TIME);
        mockShouldComputeExtraInHash(true);

        int expectedHash = expectedLocalHash(true, LAST_MODIFY_TIME);
        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashWithoutExtra = expectedLocalHash(false, LAST_MODIFY_TIME);

        Assert.assertEquals(expectedHash, hashFromCompute);
        Assert.assertNotEquals(hashWithoutExtra, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.DR));
    }

    @Test
    public void shouldComputeExtraInHash_false_shouldExcludeExtraFromLocalHash() {
        mockDrMeta();
        mockClusterLastModifyTime(LAST_MODIFY_TIME);
        mockShouldComputeExtraInHash(false);

        int expectedHash = expectedLocalHash(false, LAST_MODIFY_TIME);
        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashWithExtra = expectedLocalHash(true, LAST_MODIFY_TIME);

        Assert.assertEquals(expectedHash, hashFromCompute);
        Assert.assertNotEquals(hashWithExtra, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.DR));
    }

    @Test
    public void shouldComputeExtraInHash_false_staleLastModifyTimeShouldNotAffectLocalHash() {
        mockDrMeta();
        mockShouldComputeExtraInHash(false);
        mockClusterLastModifyTime("20200101103030002");

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashFromStaleCheckParam = expectedLocalHash(false, "20200101103030001");

        Assert.assertEquals(hashFromStaleCheckParam, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, "20200101103030001", BeaconRouteType.DR));
    }

    @Test
    public void computeClusterMetaHash_shouldMatchCheckClusterHashLocalSide_forDrRoute() {
        mockShouldComputeExtraInHash(true);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.DR, CLUSTER_TYPE);
    }

    @Test
    public void computeClusterMetaHash_shouldMatchCheckClusterHashLocalSide_forSentinelRoute() {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> shards = sentinelShards();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);

        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, CLUSTER_TYPE);
    }

    @Test
    public void computeClusterMetaHash_shouldUseMetaCacheLastModifyTimeSameAsCheckClusterHashParam() {
        mockDrMeta();
        mockClusterLastModifyTime(LAST_MODIFY_TIME);
        mockShouldComputeExtraInHash(true);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashFromCheckParam = expectedLocalHash(true, LAST_MODIFY_TIME);

        Assert.assertEquals(hashFromCheckParam, hashFromCompute);
    }

    @Test
    public void computeClusterMetaHash_shouldDifferFromCheckClusterHash_whenStaleInstanceLastModifyTime() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        String staleLastModifyTime = "20200101103030001";
        String freshLastModifyTime = "20200101103030002";
        mockClusterLastModifyTime(freshLastModifyTime);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);
        int hashFromCheckStaleParam = expectedLocalHash(true, staleLastModifyTime);

        Assert.assertNotEquals("stale ClusterInstanceInfo lastModifyTime should produce different local hash",
                hashFromCheckStaleParam, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                staleLastModifyTime, BeaconRouteType.DR);
        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void clusterConsistentWithLastModifyTimeExtra_shouldReturnConsistency() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true, LAST_MODIFY_TIME);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.DR, CLUSTER_TYPE);
    }

    @Test
    public void clusterChanged_shouldReturnInconsistency() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true, LAST_MODIFY_TIME);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(true);
        Map<String, String> clusterExtra = new HashMap<>();
        clusterExtra.put("lastModifyTime", "20200101103030000");
        Mockito.when(monitorService.getBeaconClusterExtra("xpipe", CLUSTER_ID)).thenReturn(clusterExtra);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void clusterChanged_butBeaconLastModifyTimeNewer_shouldReturnInconsistencyIgnore() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true, LAST_MODIFY_TIME);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(true);
        Map<String, String> clusterExtra = new HashMap<>();
        clusterExtra.put("lastModifyTime", "20200101103030002");
        Mockito.when(monitorService.getBeaconClusterExtra("xpipe", CLUSTER_ID)).thenReturn(clusterExtra);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY_IGNORE, status);
    }

    @Test
    public void checkBeaconLastModifyTimeDisabled_shouldSkipExtraCheckAndReturnInconsistency() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true, LAST_MODIFY_TIME);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(false);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
        Mockito.verify(monitorService, Mockito.never()).getBeaconClusterExtra("xpipe", CLUSTER_ID);
    }

    @Test
    public void getBeaconClusterExtraThrow404_shouldSkipLastModifyTimeCheckAndReturnInconsistency() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true, LAST_MODIFY_TIME);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);
        Mockito.when(checkerConfig.checkBeaconLastModifyTime()).thenReturn(true);
        Mockito.when(monitorService.getBeaconClusterExtra("xpipe", CLUSTER_ID))
                .thenThrow(new BadRequestException("404 NotFound"));

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void sentinelRouteShouldUseShardsForHashCheck() {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> shards = sentinelShards();
        int shardHash = new MonitorClusterMeta(null, shards, hashExtra(LAST_MODIFY_TIME))
                .generateHashCodeForBeaconCheck(true);
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, CLUSTER_TYPE);
    }

    @Test
    public void sentinelRouteSingleDcShouldUseOneWaySystem() {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> shards = sentinelShards();
        int shardHash = new MonitorClusterMeta(null, shards, hashExtra(LAST_MODIFY_TIME))
                .generateHashCodeForBeaconCheck(true);
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, ClusterType.SINGLE_DC, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, ClusterType.SINGLE_DC);
    }

    @Test
    public void sentinelRegisterShouldBuildShardsWithPublishMasters() {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6380", DC,
                        Collections.singleton(new HostPort("127.0.0.1", 6380)), true)
        )));
        Map<String, HostPort> shardMasters = Collections.singletonMap("shard1", new HostPort("127.0.0.1", 6380));
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, shardMasters)).thenReturn(shards);

        beaconManager.registerCluster(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME,
                BeaconRouteType.SENTINEL, shardMasters);

        Mockito.verify(monitorService).registerCluster(Mockito.eq("xpipe"), Mockito.eq(CLUSTER_ID), Mockito.isNull(),
                Mockito.eq(shards), Mockito.anyMap());
    }

    private Set<MonitorShardMeta> sentinelShards() {
        return Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", DC,
                        Collections.singleton(new HostPort("127.0.0.1", 6379)), true)
        )));
    }

    private void mockDrMeta() {
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.DR)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildDrBeaconGroups(CLUSTER_ID, DC)).thenReturn(groups);
    }

    private void mockClusterLastModifyTime(String lastModifyTime) {
        ClusterMeta clusterMeta = new ClusterMeta(CLUSTER_ID);
        clusterMeta.setLastModifiedTime(lastModifyTime);
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(new DcMeta(DC).setZone(ZONE).addCluster(clusterMeta));
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    private Map<String, String> hashExtra(String lastModifyTime) {
        return Collections.singletonMap("lastModifyTime", lastModifyTime);
    }

    private void mockShouldComputeExtraInHash(boolean enabled) {
        Mockito.when(checkerConfig.shouldComputeExtraInHash()).thenReturn(enabled);
    }

    private int expectedLocalHash(boolean includeExtra, String lastModifyTime) {
        return new MonitorClusterMeta(groups, hashExtra(lastModifyTime)).generateHashCodeForBeaconCheck(includeExtra);
    }

    private void assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType routeType, ClusterType clusterType) {
        if (routeType == BeaconRouteType.DR) {
            mockDrMeta();
        } else {
            Set<MonitorShardMeta> shards = sentinelShards();
            Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
            Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);
        }
        mockClusterLastModifyTime(LAST_MODIFY_TIME);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, clusterType, routeType);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);

        Assert.assertEquals(hashFromCompute,
                beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, clusterType, routeType));
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, clusterType, ORG_ID, LAST_MODIFY_TIME, routeType));
    }
}