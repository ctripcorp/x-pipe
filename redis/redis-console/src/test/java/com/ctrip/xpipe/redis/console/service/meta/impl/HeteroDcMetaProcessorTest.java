package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class HeteroDcMetaProcessorTest {

    @Test
    public void prepareSearchTypesShouldAddHetero() {
        var searchTypes = HeteroDcMetaProcessor.prepareSearchTypes(Collections.singleton("ONE_WAY"));
        Assert.assertTrue(searchTypes.contains(ClusterType.HETERO.toString()));
        Assert.assertTrue(searchTypes.contains(ClusterType.ONE_WAY.toString()));
    }

    @Test
    public void prepareSearchTypesShouldKeepEmpty() {
        Assert.assertTrue(HeteroDcMetaProcessor.prepareSearchTypes(Collections.emptySet()).isEmpty());
    }

    @Test
    public void postProcessShouldExpandHeteroWhenRequireAllTypes() {
        DcMeta dcMeta = new DcMeta().setId("jq");
        ClusterMeta clusterMeta = new ClusterMeta("hetero-cluster");
        clusterMeta.setType(ClusterType.HETERO.toString());
        clusterMeta.setAzGroupType(ClusterType.ONE_WAY.toString());
        dcMeta.addCluster(clusterMeta);

        HeteroDcMetaProcessor.postProcessHeteroClusters(dcMeta, Collections.emptySet());

        Assert.assertEquals(ClusterType.ONE_WAY.toString(), clusterMeta.getType());
        Assert.assertNull(clusterMeta.getAzGroupType());
    }

    @Test
    public void postProcessShouldFilterByAllowTypes() {
        DcMeta dcMeta = new DcMeta().setId("jq");
        ClusterMeta oneWay = new ClusterMeta("hetero-one-way");
        oneWay.setType(ClusterType.HETERO.toString());
        oneWay.setAzGroupType(ClusterType.ONE_WAY.toString());
        ClusterMeta singleDc = new ClusterMeta("hetero-single-dc");
        singleDc.setType(ClusterType.HETERO.toString());
        singleDc.setAzGroupType(ClusterType.SINGLE_DC.toString());
        dcMeta.addCluster(oneWay);
        dcMeta.addCluster(singleDc);

        HeteroDcMetaProcessor.postProcessHeteroClusters(dcMeta,
                Collections.singleton(ClusterType.ONE_WAY.toString()));

        Assert.assertEquals(1, dcMeta.getClusters().size());
        Assert.assertTrue(dcMeta.getClusters().containsKey("hetero-one-way"));
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), oneWay.getType());
        Assert.assertNull(oneWay.getAzGroupType());
    }
}
