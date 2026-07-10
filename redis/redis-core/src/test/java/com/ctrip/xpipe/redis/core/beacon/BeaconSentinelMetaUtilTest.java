package com.ctrip.xpipe.redis.core.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BeaconSentinelMetaUtilTest {

    @Test
    public void testResolveEffectiveClusterType() {
        ClusterMeta hetero = new ClusterMeta("cluster1");
        hetero.setType(ClusterType.HETERO.name());
        hetero.setAzGroupType(ClusterType.ONE_WAY.name());
        Assert.assertEquals(ClusterType.ONE_WAY, BeaconSentinelMetaUtil.resolveEffectiveClusterType(hetero));

        ClusterMeta singleDc = new ClusterMeta("cluster1");
        singleDc.setType(ClusterType.SINGLE_DC.name());
        Assert.assertEquals(ClusterType.SINGLE_DC, BeaconSentinelMetaUtil.resolveEffectiveClusterType(singleDc));
    }

    @Test
    public void testIsSentinelInterestedDc() {
        ClusterMeta biDirection = new ClusterMeta("cluster1");
        biDirection.setType(ClusterType.BI_DIRECTION.name());
        biDirection.setDcs("jq,oy");
        Assert.assertTrue(BeaconSentinelMetaUtil.isSentinelInterestedDc(biDirection, ClusterType.BI_DIRECTION, "jq"));
        Assert.assertFalse(BeaconSentinelMetaUtil.isSentinelInterestedDc(biDirection, ClusterType.BI_DIRECTION, "ntgxh"));

        ClusterMeta singleDc = new ClusterMeta("cluster1");
        singleDc.setType(ClusterType.SINGLE_DC.name());
        singleDc.setActiveDc("jq");
        Assert.assertTrue(BeaconSentinelMetaUtil.isSentinelInterestedDc(singleDc, ClusterType.SINGLE_DC, "jq"));
        Assert.assertFalse(BeaconSentinelMetaUtil.isSentinelInterestedDc(singleDc, ClusterType.SINGLE_DC, "oy"));
    }

    @Test
    public void testFindDcMetaIgnoreCase() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta dcMeta = new DcMeta("jq");
        xpipeMeta.addDc(dcMeta);
        Assert.assertSame(dcMeta, BeaconSentinelMetaUtil.findDcMeta(xpipeMeta, "JQ"));
    }

    @Test
    public void testIsOperatingExcluded() {
        ShardMeta shardMeta = new ShardMeta("shard1");
        Assert.assertFalse(BeaconSentinelMetaUtil.isOperatingExcluded(shardMeta));

        shardMeta.setOperatingUntil(System.currentTimeMillis() + 60_000L);
        Assert.assertTrue(BeaconSentinelMetaUtil.isOperatingExcluded(shardMeta));

        shardMeta.setOperatingUntil(DateTimeUtils.DEFAULT_OPERATING_UNTIL_MILLIS);
        Assert.assertFalse(BeaconSentinelMetaUtil.isOperatingExcluded(shardMeta));

        shardMeta.setOperatingUntil(System.currentTimeMillis() - 1L);
        Assert.assertFalse(BeaconSentinelMetaUtil.isOperatingExcluded(shardMeta));
    }

    @Test
    public void testIsBeaconCandidateForSentinelRoute() {
        DcMeta dcMeta = new DcMeta("jq");
        ClusterMeta oneWay = new ClusterMeta("cluster1");
        oneWay.setType(ClusterType.ONE_WAY.name());
        dcMeta.addCluster(oneWay);

        Assert.assertTrue(BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, "cluster1", BeaconRouteType.SENTINEL));
        Assert.assertFalse(BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, "missing", BeaconRouteType.SENTINEL));

        ClusterMeta biDirection = new ClusterMeta("cluster2");
        biDirection.setType(ClusterType.BI_DIRECTION.name());
        dcMeta.addCluster(biDirection);
        Assert.assertFalse(BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, "cluster2", BeaconRouteType.SENTINEL));

        ClusterMeta azGroupOverride = new ClusterMeta("cluster3");
        azGroupOverride.setType(ClusterType.ONE_WAY.name());
        azGroupOverride.setAzGroupType(ClusterType.BI_DIRECTION.name());
        dcMeta.addCluster(azGroupOverride);
        Assert.assertFalse(BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, "cluster3", BeaconRouteType.SENTINEL));
    }

    @Test
    public void testIsBeaconCandidateForDrRoute() {
        DcMeta dcMeta = new DcMeta("jq");
        dcMeta.setZone("SHA");
        ClusterMeta oneWay = new ClusterMeta("cluster1");
        oneWay.setType(ClusterType.ONE_WAY.name());
        oneWay.setActiveDc("jq");
        dcMeta.addCluster(oneWay);

        Set<String> supportZones = new HashSet<>(Collections.singleton("SHA"));
        Assert.assertTrue(BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, "cluster1", BeaconRouteType.DR, supportZones));
        Assert.assertFalse(BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, "cluster1", BeaconRouteType.DR, Collections.singleton("AWS")));
    }
}
