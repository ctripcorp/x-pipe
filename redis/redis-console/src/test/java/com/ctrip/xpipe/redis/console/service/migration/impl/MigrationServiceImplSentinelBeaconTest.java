package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MigrationServiceImplSentinelBeaconTest {

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();
    private static final String CLUSTER = "cluster1";

    @Mock
    private MetaCache metaCache;
    @Mock
    private BeaconManager beaconManager;
    @Mock
    private ConsoleConfig config;
    @Mock
    private XpipeMeta xpipeMeta;
    @Mock
    private DcMeta dcMeta;
    @Mock
    private ClusterMeta clusterMeta;

    private MigrationServiceImpl migrationService;
    private Map<String, ClusterMeta> clusters;

    @Before
    public void setup() {
        migrationService = new MigrationServiceImpl();
        ReflectionTestUtils.setField(migrationService, "metaCache", metaCache);
        ReflectionTestUtils.setField(migrationService, "beaconManager", beaconManager);
        ReflectionTestUtils.setField(migrationService, "config", config);

        clusters = new HashMap<>();
        Map<String, DcMeta> dcs = new HashMap<>();
        dcs.put(CURRENT_DC, dcMeta);

        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        when(xpipeMeta.getDcs()).thenReturn(dcs);
        when(dcMeta.getClusters()).thenReturn(clusters);
    }

    @Test
    public void shouldSkipWhenClusterNotInGray() {
        clusters.put(CLUSTER, clusterMeta);
        when(clusterMeta.getOrgId()).thenReturn(10);
        when(clusterMeta.getAzGroupType()).thenReturn(ClusterType.ONE_WAY.toString());
        when(config.supportSentinelBeacon(10L, CLUSTER)).thenReturn(false);

        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        assertTrue(retMessage.getMessage().contains("skipped"));
        verifyNoInteractions(beaconManager);
    }

    @Test
    public void shouldUnregisterInPreMigrateWhenEnabled() {
        clusters.put(CLUSTER, clusterMeta);
        when(clusterMeta.getOrgId()).thenReturn(10);
        when(clusterMeta.getAzGroupType()).thenReturn(ClusterType.ONE_WAY.toString());
        when(config.supportSentinelBeacon(10L, CLUSTER)).thenReturn(true);

        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        verify(beaconManager).unregisterCluster(eq(CLUSTER), eq(ClusterType.ONE_WAY), eq(10), eq(BeaconRouteType.SENTINEL));
    }

    @Test
    public void shouldRegisterInPostMigrateWhenEnabled() {
        clusters.put(CLUSTER, clusterMeta);
        when(clusterMeta.getOrgId()).thenReturn(10);
        when(clusterMeta.getAzGroupType()).thenReturn("");
        when(clusterMeta.getType()).thenReturn(ClusterType.SINGLE_DC.toString());
        when(config.supportSentinelBeacon(10L, CLUSTER)).thenReturn(true);

        RetMessage retMessage = migrationService.postMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
        verify(beaconManager).registerCluster(eq(CLUSTER), eq(ClusterType.SINGLE_DC), eq(10), anyString(), eq(BeaconRouteType.SENTINEL));
    }

    @Test
    public void shouldFailWhenClusterNotInCurrentDcMeta() {
        RetMessage retMessage = migrationService.preMigrateSentinelBeacon(CLUSTER);

        assertEquals(RetMessage.FAIL_STATE, retMessage.getState());
        assertTrue(retMessage.getMessage().contains("not found"));
        verifyNoInteractions(beaconManager);
    }
}
