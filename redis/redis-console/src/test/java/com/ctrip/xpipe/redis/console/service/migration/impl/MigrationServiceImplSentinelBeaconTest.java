package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.BeaconSentinelGrayTestConfigs;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MigrationServiceImplSentinelBeaconTest {

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();
    private static final String CLUSTER = "cluster1";
    private static final String FROM_DC = "SHARB";
    private static final String DEST_DC = "SHAXY";

    @Mock
    private MetaCache metaCache;
    @Mock
    private BeaconManager beaconManager;
    @Mock
    private ConsoleConfig config;
    @Mock
    private MigrationCluster migrationCluster;

    private MigrationServiceImpl migrationService;
    private Map<String, DcMeta> dcs;
    private XpipeMeta realXpipeMeta;

    @Before
    public void setup() {
        migrationService = new MigrationServiceImpl();
        ReflectionTestUtils.setField(migrationService, "metaCache", metaCache);
        ReflectionTestUtils.setField(migrationService, "beaconManager", beaconManager);
        ReflectionTestUtils.setField(migrationService, "config", config);

        dcs = new HashMap<>();
        XpipeMeta xpipeMeta = mock(XpipeMeta.class);
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        when(xpipeMeta.getDcs()).thenReturn(dcs);
    }

    @Test
    public void shouldSkipWhenClusterNotInGray() {
        putClusterInDc(CURRENT_DC, ClusterType.ONE_WAY);
        when(config.supportSentinelBeacon(eq(10L), eq(CLUSTER), anyString(), any(ClusterType.class))).thenReturn(false);

        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        assertTrue(retMessage.getMessage().contains("skipped"));
        verifyNoInteractions(beaconManager);
    }

    @Test
    public void shouldUnregisterInPreMigrateWhenEnabled() {
        putClusterInDc(CURRENT_DC, ClusterType.ONE_WAY);
        when(config.supportSentinelBeacon(eq(10L), eq(CLUSTER), anyString(), any(ClusterType.class))).thenReturn(true);

        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        verify(beaconManager).unregisterCluster(eq(CLUSTER), eq(CURRENT_DC), eq(ClusterType.ONE_WAY), eq(10),
                eq(BeaconRouteType.SENTINEL));
    }

    @Test
    public void shouldRegisterInPostMigrateWhenEnabled() {
        putClusterInDc(CURRENT_DC, ClusterType.SINGLE_DC);
        when(config.supportSentinelBeacon(eq(10L), eq(CLUSTER), anyString(), any(ClusterType.class))).thenReturn(true);

        Map<String, HostPort> shardMasters = new HashMap<>();
        shardMasters.put("shard1", new HostPort("127.0.0.1", 6379));

        RetMessage retMessage = migrationService.postMigrateSentinelBeacon(CLUSTER, shardMasters);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        verify(beaconManager).registerCluster(eq(CLUSTER), eq(CURRENT_DC), eq(ClusterType.SINGLE_DC), eq(10),
                eq(BeaconRouteType.SENTINEL), eq(shardMasters));
    }

    @Test
    public void shouldFailWhenClusterNotInCurrentDcMeta() {
        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.FAIL_STATE, retMessage.getState());
        assertTrue(retMessage.getMessage().contains("not found"));
        verifyNoInteractions(beaconManager);
    }

    @Test
    public void shouldMigrateOnlyWhenTargetDcInIdcGray() {
        installIdcGrayConfig(FROM_DC);
        putRealClusterInDc(FROM_DC);
        putRealClusterInDc(DEST_DC);
        when(migrationCluster.clusterName()).thenReturn(CLUSTER);

        assertTrue(migrationService.shouldMigrateSentinelBeacon(migrationCluster, FROM_DC));
        assertFalse(migrationService.shouldMigrateSentinelBeacon(migrationCluster, DEST_DC));
    }

    @Test
    public void preMigrateShouldUnregisterFromDcWhenOnlyFromDcInIdcGray() {
        installIdcGrayConfig(FROM_DC);
        putRealClusterInDc(FROM_DC);
        putRealClusterInDc(DEST_DC);
        when(migrationCluster.clusterName()).thenReturn(CLUSTER);
        when(migrationCluster.fromDc()).thenReturn(FROM_DC);

        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(migrationCluster);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        verify(beaconManager).unregisterCluster(eq(CLUSTER), eq(FROM_DC), eq(ClusterType.ONE_WAY), eq(10),
                eq(BeaconRouteType.SENTINEL));
    }

    @Test
    public void postMigrateShouldSkipWhenDestDcNotInIdcGray() {
        installIdcGrayConfig(FROM_DC);
        putRealClusterInDc(FROM_DC);
        putRealClusterInDc(DEST_DC);
        when(migrationCluster.clusterName()).thenReturn(CLUSTER);
        when(migrationCluster.destDc()).thenReturn(DEST_DC);

        Map<String, HostPort> shardMasters = Collections.singletonMap("shard1", new HostPort("127.0.0.1", 6379));
        RetMessage retMessage = migrationService.postMigrateSentinelBeacon(migrationCluster, shardMasters);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        assertTrue(retMessage.getMessage().contains("skipped"));
        verifyNoInteractions(beaconManager);
    }

    @Test
    public void postMigrateShouldRegisterWhenDestDcInIdcGray() {
        installIdcGrayConfig(DEST_DC);
        putRealClusterInDc(FROM_DC);
        putRealClusterInDc(DEST_DC);
        when(migrationCluster.clusterName()).thenReturn(CLUSTER);
        when(migrationCluster.destDc()).thenReturn(DEST_DC);

        Map<String, HostPort> shardMasters = Collections.singletonMap("shard1", new HostPort("127.0.0.1", 6379));
        RetMessage retMessage = migrationService.postMigrateSentinelBeacon(migrationCluster, shardMasters);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        verify(beaconManager).registerCluster(eq(CLUSTER), eq(DEST_DC), eq(ClusterType.ONE_WAY), eq(10),
                eq(BeaconRouteType.SENTINEL), eq(shardMasters));
    }

    private void installIdcGrayConfig(String grayDc) {
        Map<String, Set<String>> idcGray = new HashMap<>();
        idcGray.put(grayDc.toLowerCase(), new HashSet<>(Collections.singletonList(ClusterType.ONE_WAY.name())));
        ConsoleConfig idcConfig = BeaconSentinelGrayTestConfigs.withIdcClusterTypes(idcGray);
        ReflectionTestUtils.setField(migrationService, "config", idcConfig);

        realXpipeMeta = new XpipeMeta();
        when(metaCache.getXpipeMeta()).thenReturn(realXpipeMeta);
    }

    private void putClusterInDc(String dc, ClusterType clusterType) {
        ClusterMeta clusterMeta = mock(ClusterMeta.class);
        when(clusterMeta.getOrgId()).thenReturn(10);
        when(clusterMeta.getType()).thenReturn(clusterType.toString());
        when(clusterMeta.getAzGroupType()).thenReturn("");
        DcMeta dcMeta = mock(DcMeta.class);
        Map<String, ClusterMeta> clusters = new HashMap<>();
        clusters.put(CLUSTER, clusterMeta);
        when(dcMeta.getClusters()).thenReturn(clusters);
        dcs.put(dc, dcMeta);
    }

    private void putRealClusterInDc(String dc) {
        ClusterMeta clusterMeta = new ClusterMeta(CLUSTER)
                .setOrgId(10)
                .setType(ClusterType.ONE_WAY.name())
                .setActiveDc(dc);
        DcMeta dcMeta = new DcMeta(dc);
        dcMeta.addCluster(clusterMeta);
        realXpipeMeta.addDc(dcMeta);
    }
}
