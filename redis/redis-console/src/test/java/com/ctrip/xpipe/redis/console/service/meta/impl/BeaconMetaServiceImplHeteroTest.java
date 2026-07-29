package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.core.beacon.BeaconSentinelMetaUtil;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BeaconMetaServiceImplHeteroTest {

    private MetaCache metaCache;

    private ConsoleCommonConfig config;

    private BeaconMetaServiceImpl beaconMetaService;

    @Before
    public void setUp() {
        metaCache = Mockito.mock(MetaCache.class);
        config = Mockito.mock(ConsoleCommonConfig.class);
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.emptySet());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(dualOneWayMeta());
        stubCrossRegionAgainst(dualOneWayMeta());
        beaconMetaService = new BeaconMetaServiceImpl(metaCache, config);
    }

    private void stubCrossRegionAgainst(XpipeMeta xpipeMeta) {
        Mockito.when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> {
            String activeDc = invocation.getArgument(0, String.class);
            String dc = invocation.getArgument(1, String.class);
            DcMeta activeMeta = xpipeMeta.getDcs().get(activeDc);
            DcMeta otherMeta = xpipeMeta.getDcs().get(dc);
            if (activeMeta == null || otherMeta == null) {
                return true;
            }
            return !activeMeta.getZone().equals(otherMeta.getZone());
        });
    }

    @Test
    public void buildDrBeaconGroupsByAnchorDcShouldOnlyIncludeSameRegionDcs() {
        Set<MonitorGroupMeta> jqGroups = beaconMetaService.buildDrBeaconGroups("hetero-dual-oneway", "jq");
        Assert.assertEquals(4, jqGroups.size());
        Assert.assertTrue(jqGroups.stream().allMatch(group -> group.getIdc().equals("jq") || group.getIdc().equals("oy")));

        Set<MonitorGroupMeta> fraGroups = beaconMetaService.buildDrBeaconGroups("hetero-dual-oneway", "fra");
        Assert.assertEquals(1, fraGroups.size());
        Assert.assertEquals("fra", fraGroups.iterator().next().getIdc());
    }

    @Test
    public void buildDrBeaconGroupsForEachRegionShouldCoverAllGroups() {
        Set<MonitorGroupMeta> allGroups = new HashSet<>();
        allGroups.addAll(beaconMetaService.buildDrBeaconGroups("hetero-dual-oneway", "jq"));
        allGroups.addAll(beaconMetaService.buildDrBeaconGroups("hetero-dual-oneway", "fra"));
        Assert.assertEquals(5, allGroups.size());
        Assert.assertFalse(allGroups.stream().anyMatch(group -> "fra".equals(group.getIdc()) && group.getName().contains("jq")));
    }

    @Test
    public void buildDrBeaconGroupsShouldReturnEmptyWhenActiveDcNotInSupportZones() {
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("FRA"));
        Mockito.when(metaCache.isDcInRegion(Mockito.eq("jq"), Mockito.eq("FRA"))).thenReturn(false);

        Assert.assertTrue(beaconMetaService.buildDrBeaconGroups("hetero-dual-oneway", "jq").isEmpty());
    }

    @Test
    public void crossRegionOneWayDrShouldExcludeCrossRegionBackup() {
        XpipeMeta crossRegionMeta = crossRegionOneWayMeta();
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(crossRegionMeta);
        stubCrossRegionAgainst(crossRegionMeta);

        Set<MonitorGroupMeta> groups = beaconMetaService.buildDrBeaconGroups("hetero-cross-region", "jq");
        Assert.assertFalse(groups.isEmpty());
        Assert.assertTrue(groups.stream().allMatch(group ->
                "jq".equals(group.getIdc()) || "oy".equals(group.getIdc())));
        Assert.assertFalse(groups.stream().anyMatch(group -> "fra".equals(group.getIdc())));
    }

    @Test
    public void crossRegionOneWaySentinelShouldOnlyInterestActiveDc() {
        XpipeMeta crossRegionMeta = crossRegionOneWayMeta();
        ClusterMeta jqCluster = crossRegionMeta.getDcs().get("jq").getClusters().get("hetero-cross-region");
        ClusterType effective = BeaconSentinelMetaUtil.resolveEffectiveClusterType(jqCluster);
        Assert.assertEquals(ClusterType.ONE_WAY, effective);

        Assert.assertTrue(BeaconSentinelMetaUtil.isSentinelInterestedDc(jqCluster, effective, "jq"));
        Assert.assertFalse(BeaconSentinelMetaUtil.isSentinelInterestedDc(jqCluster, effective, "oy"));
        Assert.assertFalse(BeaconSentinelMetaUtil.isSentinelInterestedDc(jqCluster, effective, "fra"));

        Assert.assertTrue(BeaconSentinelMetaUtil.isBeaconCandidate(
                crossRegionMeta.getDcs().get("jq"), "hetero-cross-region", BeaconRouteType.DR, config));
        Assert.assertFalse(BeaconSentinelMetaUtil.isBeaconCandidate(
                crossRegionMeta.getDcs().get("fra"), "hetero-cross-region", BeaconRouteType.DR, config));
    }

    private XpipeMeta crossRegionOneWayMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jq = new DcMeta("jq").setZone("SHA");
        DcMeta oy = new DcMeta("oy").setZone("SHA");
        DcMeta fra = new DcMeta("fra").setZone("FRA");

        ClusterMeta cross = heteroOneWayCluster("hetero-cross-region", "jq", "oy,fra");
        jq.addCluster(copyCluster(cross));
        oy.addCluster(copyCluster(cross));
        fra.addCluster(copyCluster(cross));
        return xpipeMeta.addDc(jq).addDc(oy).addDc(fra);
    }

    private XpipeMeta dualOneWayMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jq = new DcMeta("jq").setZone("SHA");
        DcMeta oy = new DcMeta("oy").setZone("SHA");
        DcMeta fra = new DcMeta("fra").setZone("FRA");

        ClusterMeta shaCluster = heteroOneWayCluster("hetero-dual-oneway", "jq", "oy");
        jq.addCluster(copyCluster(shaCluster));
        oy.addCluster(copyCluster(shaCluster));

        ClusterMeta fraCluster = heteroOneWayCluster("hetero-dual-oneway", "fra", "");
        fra.addCluster(copyCluster(fraCluster));

        return xpipeMeta.addDc(jq).addDc(oy).addDc(fra);
    }

    private ClusterMeta heteroOneWayCluster(String clusterName, String activeDc, String backupDcs) {
        ClusterMeta clusterMeta = new ClusterMeta(clusterName);
        clusterMeta.setType(ClusterType.HETERO.toString());
        clusterMeta.setAzGroupType(ClusterType.ONE_WAY.toString());
        clusterMeta.setActiveDc(activeDc);
        clusterMeta.setBackupDcs(backupDcs);
        clusterMeta.addShard(buildShard(clusterName + "_" + activeDc + "_1", activeDc));
        if ("jq".equals(activeDc) && backupDcs != null && backupDcs.contains("oy")) {
            clusterMeta.addShard(buildShard(clusterName + "_oy_1", "oy"));
        }
        return clusterMeta;
    }

    private ClusterMeta copyCluster(ClusterMeta source) {
        ClusterMeta clusterMeta = new ClusterMeta(source.getId());
        clusterMeta.setType(source.getType());
        clusterMeta.setAzGroupType(source.getAzGroupType());
        clusterMeta.setActiveDc(source.getActiveDc());
        clusterMeta.setBackupDcs(source.getBackupDcs());
        source.getShards().values().forEach(clusterMeta::addShard);
        return clusterMeta;
    }

    private ShardMeta buildShard(String shardName, String dc) {
        ShardMeta shardMeta = new ShardMeta(shardName);
        com.ctrip.xpipe.redis.core.entity.RedisMeta master =
                new com.ctrip.xpipe.redis.core.entity.RedisMeta().setIp("10.0.0.1").setPort(6379).setMaster("127.0.0.1");
        com.ctrip.xpipe.redis.core.entity.RedisMeta backup =
                new com.ctrip.xpipe.redis.core.entity.RedisMeta().setIp("10.0.0.2").setPort(6379).setMaster("");
        if ("jq".equals(dc)) {
            shardMeta.addRedis(master).addRedis(backup);
        } else {
            shardMeta.addRedis(backup);
        }
        return shardMeta;
    }
}
