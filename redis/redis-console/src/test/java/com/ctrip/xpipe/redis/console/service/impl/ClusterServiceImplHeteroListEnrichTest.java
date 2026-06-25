package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClusterServiceImplHeteroListEnrichTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterServiceImpl clusterService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

    @Test
    public void testEnrichHeteroDualOneWayClusterListFields() {
        ClusterTbl cluster = clusterService.find("hetero-dual-oneway");
        Assert.assertNotNull(cluster);
        Assert.assertEquals(ClusterType.HETERO.name(), cluster.getClusterType());

        clusterService.enrichHeteroClustersForList(Collections.singletonList(cluster));
        Assert.assertEquals("ONE_WAY:fra / ONE_WAY:jq", cluster.getHeteroActiveDcSummary());
        Assert.assertEquals("fra", cluster.getHeteroDefaultFromDc());
        Assert.assertEquals(Arrays.asList(3L, 1L), cluster.getHeteroActiveDcIds());
    }

    @Test
    public void testFindAllClustersWithOrgInfoEnrichesHeteroCluster() {
        List<ClusterTbl> clusters = clusterService.findAllClustersWithOrgInfo();
        ClusterTbl hetero = clusters.stream()
                .filter(c -> "hetero-dual-oneway".equals(c.getClusterName()))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(hetero);
        Assert.assertEquals("ONE_WAY:fra / ONE_WAY:jq", hetero.getHeteroActiveDcSummary());
        Assert.assertEquals("fra", hetero.getHeteroDefaultFromDc());
    }

    @Test
    public void testFindClusterAndOrgEnrichesDefaultFromDc() {
        ClusterTbl cluster = clusterService.findClusterAndOrg("hetero-dual-oneway");
        Assert.assertNotNull(cluster);
        Assert.assertEquals("fra", cluster.getHeteroDefaultFromDc());
        Assert.assertFalse(cluster.getHeteroActiveDcSummary().isEmpty());
    }

    @Test
    public void testOneWayClusterListFieldsUnchanged() {
        ClusterTbl cluster = clusterService.find("cluster1");
        clusterService.enrichHeteroClustersForList(Collections.singletonList(cluster));
        Assert.assertNull(cluster.getHeteroActiveDcSummary());
        Assert.assertNull(cluster.getHeteroDefaultFromDc());
    }
}
