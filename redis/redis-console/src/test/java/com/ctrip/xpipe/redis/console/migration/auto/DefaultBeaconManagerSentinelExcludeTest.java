package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class DefaultBeaconManagerSentinelExcludeTest extends AbstractConsoleIntegrationTest {

    private static final String CLUSTER = "cluster1";

    private static final String DC = "jq";

    private static final int ORG_ID = 1;

    private static final String LAST_MODIFY_TIME = "20200101103030001";

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private MonitorService monitorService;

    private MetaCache metaCache;

    private ConsoleCommonConfig config;

    private BeaconMetaServiceImpl beaconMetaService;

    private DefaultBeaconManager beaconManager;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "multi-zone-meta.xml";
    }

    @Before
    public void setupDefaultBeaconManagerSentinelExcludeTest() {
        MockitoAnnotations.initMocks(this);
        metaCache = Mockito.mock(MetaCache.class);
        config = Mockito.mock(ConsoleCommonConfig.class);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(MetaCloneFacade.INSTANCE.clone(getXpipeMeta()));
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("SHA"));
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.eq("SHA"))).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgument(0, String.class);
            String backupDc = invocation.getArgument(1, String.class);
            XpipeMeta xpipeMeta = getXpipeMeta();
            return !xpipeMeta.getDcs().get(activeDc).getZone().equals(xpipeMeta.getDcs().get(backupDc).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        beaconMetaService = new BeaconMetaServiceImpl(metaCache, config);
        beaconManager = new DefaultBeaconManager(monitorManager, beaconMetaService, checkerConfig, metaCache);

        DcMeta dcMeta = getXpipeMeta().getDcs().get(DC);
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER, dcMeta.getZone(), BeaconRouteType.SENTINEL))
                .thenReturn(monitorService);
        Mockito.when(checkerConfig.shouldComputeExtraInHash()).thenReturn(true);
        mockClusterLastModifyTime(LAST_MODIFY_TIME);
    }

    @Test
    public void sentinelHashShouldShrinkWhenShardExcluded() {
        int fullHash = beaconManager.computeClusterMetaHash(CLUSTER, DC, ClusterType.ONE_WAY, BeaconRouteType.SENTINEL);

        excludeShard("shard1");
        int reducedHash = beaconManager.computeClusterMetaHash(CLUSTER, DC, ClusterType.ONE_WAY, BeaconRouteType.SENTINEL);

        Assert.assertNotEquals(fullHash, reducedHash);
    }

    @Test
    public void drHashShouldIgnoreOperatingUntil() {
        int fullHash = beaconManager.computeClusterMetaHash(CLUSTER, DC, ClusterType.ONE_WAY, BeaconRouteType.DR);

        excludeShard("shard1");
        int hashAfterExclude = beaconManager.computeClusterMetaHash(CLUSTER, DC, ClusterType.ONE_WAY, BeaconRouteType.DR);

        Assert.assertEquals(fullHash, hashAfterExclude);
    }

    @Test
    public void checkClusterHashShouldBeInconsistentWhenBeaconHasFullShards() throws Exception {
        Set<MonitorShardMeta> fullShards = beaconMetaService.buildSentinelBeaconShards(CLUSTER, DC, Collections.emptyMap());
        int beaconHash = new MonitorClusterMeta(null, fullShards, Collections.singletonMap("lastModifyTime", LAST_MODIFY_TIME))
                .generateHashCodeForBeaconCheck(true);

        excludeShard("shard1");
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER)).thenReturn(beaconHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER, DC, ClusterType.ONE_WAY, ORG_ID,
                LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);
        Assert.assertEquals(BeaconCheckStatus.INCONSISTENCY, status);
    }

    @Test
    public void registerClusterShouldUseExcludedShards() throws Exception {
        excludeShard("shard1");
        Set<MonitorShardMeta> expectedShards = beaconMetaService.buildSentinelBeaconShards(CLUSTER, DC,
                Collections.emptyMap());
        Assert.assertEquals(1, expectedShards.size());

        beaconManager.registerCluster(CLUSTER, DC, ClusterType.ONE_WAY, ORG_ID, LAST_MODIFY_TIME,
                BeaconRouteType.SENTINEL, Collections.emptyMap());

        ArgumentCaptor<Set> shardsCaptor = ArgumentCaptor.forClass(Set.class);
        Mockito.verify(monitorService).registerCluster(Mockito.eq("xpipe"), Mockito.eq(CLUSTER), Mockito.isNull(),
                shardsCaptor.capture(), Mockito.anyMap());
        Assert.assertEquals(expectedShards, shardsCaptor.getValue());
    }

    private void excludeShard(String shardName) {
        XpipeMeta xpipeMeta = MetaCloneFacade.INSTANCE.clone(getXpipeMeta());
        xpipeMeta.getDcs().get(DC).getClusters().get(CLUSTER).setLastModifiedTime(LAST_MODIFY_TIME);
        xpipeMeta.getDcs().get(DC).getClusters().get(CLUSTER).getShards().get(shardName)
                .setOperatingUntil(System.currentTimeMillis() + 60_000L);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    private void mockClusterLastModifyTime(String lastModifyTime) {
        XpipeMeta xpipeMeta = MetaCloneFacade.INSTANCE.clone(getXpipeMeta());
        xpipeMeta.getDcs().get(DC).getClusters().get(CLUSTER).setLastModifiedTime(lastModifyTime);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }
}
