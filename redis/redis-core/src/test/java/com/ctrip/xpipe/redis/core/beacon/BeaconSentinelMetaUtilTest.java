package com.ctrip.xpipe.redis.core.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.junit.Assert;
import org.junit.Test;

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
}
