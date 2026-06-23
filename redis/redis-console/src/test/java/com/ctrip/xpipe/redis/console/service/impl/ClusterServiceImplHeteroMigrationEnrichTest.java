package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ClusterServiceImplHeteroMigrationEnrichTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterServiceImpl clusterService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

    @Test
    public void testEnrichMigrationClustersForActiveDc() {
        ClusterTbl cluster = clusterService.find("hetero-dual-oneway");
        Assert.assertNotNull(cluster);
        Assert.assertEquals(ClusterType.HETERO.name(), cluster.getClusterType());

        clusterService.enrichMigrationClustersForActiveDc(Collections.singletonList(cluster), "jq");
        Assert.assertEquals(1L, cluster.getMigrationActiveDcId());
        Assert.assertEquals(23L, cluster.getMigrationAzGroupClusterId());

        ClusterTbl fraView = clusterService.find("hetero-dual-oneway");
        clusterService.enrichMigrationClustersForActiveDc(Collections.singletonList(fraView), "fra");
        Assert.assertEquals(3L, fraView.getMigrationActiveDcId());
        Assert.assertEquals(24L, fraView.getMigrationAzGroupClusterId());
    }

    @Test
    public void testFindMigrationActiveClustersByDcName() {
        List<ClusterTbl> jqClusters = clusterService.findMigrationActiveClustersByDcName("jq");
        ClusterTbl jqHetero = jqClusters.stream()
                .filter(c -> "hetero-dual-oneway".equals(c.getClusterName()))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(jqHetero);
        Assert.assertEquals(1L, jqHetero.getMigrationActiveDcId());
        Assert.assertEquals(23L, jqHetero.getMigrationAzGroupClusterId());
        Assert.assertFalse(jqClusters.stream().anyMatch(c -> "singleDcCluster".equals(c.getClusterName())));

        List<ClusterTbl> fraClusters = clusterService.findMigrationActiveClustersByDcName("fra");
        ClusterTbl fraHetero = fraClusters.stream()
                .filter(c -> "hetero-dual-oneway".equals(c.getClusterName()))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(fraHetero);
        Assert.assertEquals(3L, fraHetero.getMigrationActiveDcId());
        Assert.assertEquals(24L, fraHetero.getMigrationAzGroupClusterId());
    }

    @Test
    public void testFindActiveClustersByDcNameIncludesSingleDc() {
        List<ClusterTbl> jqClusters = clusterService.findActiveClustersByDcName("jq");
        Assert.assertTrue(jqClusters.stream().anyMatch(c -> "singleDcCluster".equals(c.getClusterName())));
    }
}
