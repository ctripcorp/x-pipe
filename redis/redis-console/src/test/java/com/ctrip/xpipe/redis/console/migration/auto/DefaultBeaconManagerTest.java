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
    private MonitorService monitorService;

    private DefaultBeaconManager beaconManager;

    private Set<MonitorGroupMeta> groups;
    private int localHash;

    @Before
    public void setUp() {
        beaconManager = new DefaultBeaconManager(monitorManager, beaconMetaService, checkerConfig);

        groups = Collections.singleton(new MonitorGroupMeta("shard1", "jq",
                Collections.singleton(new HostPort("127.0.0.1", 6379)), true));
        localHash = new MonitorClusterMeta(groups).generateHashCodeForBeaconCheck();
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
        int shardHash = new MonitorClusterMeta(null, shards, null).generateHashCodeForBeaconCheck();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconShards(CLUSTER_ID, "jq")).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, CLUSTER_TYPE, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
    }

    @Test
    public void sentinelRouteSingleDcShouldUseOneWaySystem() {
        Set<MonitorShardMeta> shards = Collections.singleton(new MonitorShardMeta("shard1", Arrays.asList(
                new MonitorGroupMeta("127.0.0.1:6379", "jq",
                        Collections.singleton(new HostPort("127.0.0.1", 6379)), true)
        )));
        int shardHash = new MonitorClusterMeta(null, shards, null).generateHashCodeForBeaconCheck();
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.SENTINEL)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconShards(CLUSTER_ID, "jq")).thenReturn(shards);
        Mockito.when(monitorService.getBeaconClusterHash("xpipe", CLUSTER_ID)).thenReturn(shardHash);

        BeaconCheckStatus status = beaconManager.checkClusterHash(CLUSTER_ID, ClusterType.SINGLE_DC, ORG_ID, LAST_MODIFY_TIME, BeaconRouteType.SENTINEL);

        Assert.assertEquals(BeaconCheckStatus.CONSISTENCY, status);
    }

    private void mockDrMeta() {
        Mockito.when(monitorManager.get(ORG_ID, CLUSTER_ID, BeaconRouteType.DR)).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildBeaconGroups(CLUSTER_ID)).thenReturn(groups);
    }
}
