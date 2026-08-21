package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
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

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultBeaconManagerTest extends AbstractConsoleTest {

    private static final String CLUSTER_ID = "cluster1";
    private static final int ORG_ID = 1;
    private static final ClusterType CLUSTER_TYPE = ClusterType.ONE_WAY;
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
    public void shouldComputeExtraInHash_true_shouldComputeLocalHashConsistently() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);

        int expectedHash = expectedLocalHash(true);
        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);

        Assert.assertEquals(expectedHash, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, BeaconRouteType.DR));
    }

    @Test
    public void shouldComputeExtraInHash_false_shouldComputeLocalHashConsistently() {
        mockDrMeta();
        mockShouldComputeExtraInHash(false);

        int expectedHash = expectedLocalHash(false);
        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);

        Assert.assertEquals(expectedHash, hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, BeaconRouteType.DR));
    }

    @Test
    public void shouldComputeExtraInHash_false_localHashStableRegardlessOfExtra() {
        mockDrMeta();
        mockShouldComputeExtraInHash(false);

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, CLUSTER_TYPE, BeaconRouteType.DR);

        Assert.assertEquals(expectedLocalHash(false), hashFromCompute);

        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, BeaconRouteType.DR));
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
    public void clusterConsistent_shouldReturnConsistency() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.DR, CLUSTER_TYPE);
    }

    @Test
    public void clusterChanged_shouldReturnInconsistency() {
        mockDrMeta();
        mockShouldComputeExtraInHash(true);
        int localHash = expectedLocalHash(true);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(localHash + 1);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID, BeaconRouteType.DR);

        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void sentinelRouteShouldUseShardsForHashCheck() {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> shards = sentinelShards();
        int shardHash = new MonitorClusterMeta(null, shards, Collections.emptyMap())
                .generateHashCodeForBeaconCheck(true);
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
        assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType.SENTINEL, CLUSTER_TYPE);
    }

    @Test
    public void sentinelRouteSingleDcShouldUseOneWaySystem() {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> shards = sentinelShards();
        int shardHash = new MonitorClusterMeta(null, shards, Collections.emptyMap())
                .generateHashCodeForBeaconCheck(true);
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, ClusterType.SINGLE_DC, ORG_ID,
                BeaconRouteType.SENTINEL);

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

        beaconManager.registerCluster(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                BeaconRouteType.SENTINEL, shardMasters);

        Mockito.verify(monitorService).registerCluster(Mockito.eq("xpipe"), Mockito.eq(CLUSTER_ID), Mockito.isNull(),
                Mockito.eq(shards), Mockito.anyMap());
    }

    @Test
    public void sentinelRouteShouldDetectInconsistencyWhenShardExcluded() {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> fullShards = sentinelShardsWithTwoShards();
        Set<MonitorShardMeta> reducedShards = sentinelShards();
        int beaconHash = new MonitorClusterMeta(null, fullShards, Collections.emptyMap())
                .generateHashCodeForBeaconCheck(true);
        int localHash = new MonitorClusterMeta(null, reducedShards, Collections.emptyMap())
                .generateHashCodeForBeaconCheck(true);
        Assert.assertNotEquals(beaconHash, localHash);

        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap()))
                .thenReturn(reducedShards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(beaconHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                BeaconRouteType.SENTINEL);
        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void sentinelInconsistentShouldRegisterReducedShards() throws Exception {
        mockShouldComputeExtraInHash(true);
        Set<MonitorShardMeta> fullShards = sentinelShardsWithTwoShards();
        Set<MonitorShardMeta> reducedShards = sentinelShards();

        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap()))
                .thenReturn(reducedShards);

        beaconManager.registerCluster(CLUSTER_ID, DC, CLUSTER_TYPE, ORG_ID,
                BeaconRouteType.SENTINEL, Collections.emptyMap());

        Mockito.verify(monitorService).registerCluster(Mockito.eq("xpipe"), Mockito.eq(CLUSTER_ID), Mockito.isNull(),
                Mockito.eq(reducedShards), Mockito.anyMap());
    }

    private Set<MonitorShardMeta> sentinelShardsWithTwoShards() {
        return new HashSet<>(Arrays.asList(
                new MonitorShardMeta("shard1", Arrays.asList(
                        new MonitorGroupMeta("127.0.0.1:6379", DC,
                                Collections.singleton(new HostPort("127.0.0.1", 6379)), true))),
                new MonitorShardMeta("shard2", Arrays.asList(
                        new MonitorGroupMeta("127.0.0.1:6381", DC,
                                Collections.singleton(new HostPort("127.0.0.1", 6381)), true)))
        ));
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

    private void mockShouldComputeExtraInHash(boolean enabled) {
        Mockito.when(checkerConfig.shouldComputeExtraInHash()).thenReturn(enabled);
    }

    private int expectedLocalHash(boolean includeExtra) {
        return new MonitorClusterMeta(groups, Collections.emptyMap()).generateHashCodeForBeaconCheck(includeExtra);
    }

    private void assertLocalHashConsistentBetweenCheckAndCompute(BeaconRouteType routeType, ClusterType clusterType) {
        if (routeType == BeaconRouteType.DR) {
            mockDrMeta();
        } else {
            Set<MonitorShardMeta> shards = sentinelShards();
            Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, ZONE, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
            Mockito.when(beaconMetaService.buildSentinelBeaconShards(CLUSTER_ID, DC, Collections.emptyMap())).thenReturn(shards);
        }

        int hashFromCompute = beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, clusterType, routeType);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(hashFromCompute);

        Assert.assertEquals(hashFromCompute,
                beaconManager.computeClusterMetaHash(CLUSTER_ID, DC, clusterType, routeType));
        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY,
                beaconManager.checkClusterHash(CLUSTER_ID, DC, clusterType, ORG_ID, routeType));
    }
}
