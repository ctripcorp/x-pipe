package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.beacon.BeaconConstant;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

@RunWith(MockitoJUnitRunner.class)
public class BeaconCheckConfigServiceImplTest {

    @Mock
    private DcClusterShardService dcClusterShardService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private MetaCache metaCache;

    @InjectMocks
    private BeaconCheckConfigServiceImpl beaconCheckConfigService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setClusterOrgId(1L);
        clusterTbl.setClusterType(ClusterType.SINGLE_DC.name());
        Mockito.when(clusterService.find("cluster1")).thenReturn(clusterTbl);
        Mockito.when(consoleConfig.supportSentinelBeacon(1L, "cluster1")).thenReturn(true);
        Mockito.doReturn(1).when(dcClusterShardService).batchUpdateOperatingUntil(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyList(), Mockito.any(Date.class));

        ClusterMeta clusterMeta = new ClusterMeta("cluster1");
        clusterMeta.setType(ClusterType.SINGLE_DC.name());
        clusterMeta.setActiveDc("jq");
        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.addCluster(clusterMeta);
        DcMeta oyDcMeta = new DcMeta("oy");
        oyDcMeta.addCluster(MetaCloneFacade.INSTANCE.clone(clusterMeta));
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dcMeta);
        xpipeMeta.addDc(oyDcMeta);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    @Test
    public void testStopBeaconCheck() throws Exception {
        beaconCheckConfigService.stopBeaconCheck("cluster1", "jq",
                Collections.singletonList("shard1"), 30);
        Mockito.verify(dcClusterShardService).batchUpdateOperatingUntil(Mockito.eq("jq"), Mockito.eq("cluster1"),
                Mockito.eq(Collections.singletonList("shard1")), Mockito.argThat(date ->
                        date.getTime() > System.currentTimeMillis()));
    }

    @Test
    public void testStartBeaconCheck() throws Exception {
        beaconCheckConfigService.startBeaconCheck("cluster1", "jq", Collections.singletonList("shard1"));
        Mockito.verify(dcClusterShardService).batchUpdateOperatingUntil(Mockito.eq("jq"), Mockito.eq("cluster1"),
                Mockito.eq(Collections.singletonList("shard1")), Mockito.eq(BeaconConstant.DEFAULT_OPERATING_UNTIL));
    }

    @Test
    public void testStopBeaconCheckRejectsEmptyShards() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("shards can not be empty");
        beaconCheckConfigService.stopBeaconCheck("cluster1", "jq", Collections.emptyList(), 30);
    }

    @Test
    public void testStopBeaconCheckRejectsBlankShardName() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("shard name can not be empty");
        beaconCheckConfigService.stopBeaconCheck("cluster1", "jq", Arrays.asList("shard1", ""), 30);
    }

    @Test
    public void testStopBeaconCheckRejectsNonSentinelBeaconCluster() throws Exception {
        Mockito.when(consoleConfig.supportSentinelBeacon(1L, "cluster1")).thenReturn(false);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("cluster cluster1 is not managed by beacon sentinel mode");
        beaconCheckConfigService.stopBeaconCheck("cluster1", "jq", Collections.singletonList("shard1"), 30);
    }

    @Test
    public void testStopBeaconCheckRejectsWrongDc() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("dc oy is not a sentinel beacon interested dc for cluster cluster1");
        beaconCheckConfigService.stopBeaconCheck("cluster1", "oy", Collections.singletonList("shard1"), 30);
    }
}
