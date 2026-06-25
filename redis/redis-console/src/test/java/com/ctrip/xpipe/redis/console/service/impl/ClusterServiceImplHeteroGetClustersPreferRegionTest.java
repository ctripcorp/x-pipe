package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.impl.AzGroupCacheImpl;
import com.ctrip.xpipe.redis.console.dto.ClusterDTO;
import com.ctrip.xpipe.redis.console.model.consoleportal.ClusterDcGroupModel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterServiceImplHeteroGetClustersPreferRegionTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterServiceImpl clusterService;

    @Autowired
    private AzGroupCache azGroupCache;

    @Before
    public void refreshAzGroupCacheAfterDbReload() throws Exception {
        if (!(azGroupCache instanceof AzGroupCacheImpl)) {
            return;
        }
        Field models = AzGroupCacheImpl.class.getDeclaredField("azGroupModels");
        models.setAccessible(true);
        models.set(azGroupCache, null);
        Field idMap = AzGroupCacheImpl.class.getDeclaredField("idAzGroupMap");
        idMap.setAccessible(true);
        idMap.set(azGroupCache, null);
        azGroupCache.getAllAzGroup();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

    @Test
    public void testGetClustersPreferRegionSha() {
        ClusterDTO hetero = findHeteroDualOneWayCluster(clusterService.getClusters(ClusterType.HETERO.name(), "SHA"));
        Assert.assertNotNull(hetero);
        Assert.assertEquals("jq", hetero.getActiveAz());
        Assert.assertEquals(Arrays.asList("jq", "oy"), hetero.getAzs());
    }

    @Test
    public void testGetClustersPreferRegionFra() {
        ClusterDTO hetero = findHeteroDualOneWayCluster(clusterService.getClusters(ClusterType.HETERO.name(), "FRA"));
        Assert.assertNotNull(hetero);
        Assert.assertEquals("fra", hetero.getActiveAz());
        Assert.assertEquals(Arrays.asList("fra", "fra-ali"), hetero.getAzs());
    }

    @Test
    public void testGetClustersPreferRegionFallbackWhenRegionMissing() {
        ClusterDTO hetero = findHeteroDualOneWayCluster(clusterService.getClusters(ClusterType.HETERO.name(), "XXX"));
        Assert.assertNotNull(hetero);
        Assert.assertEquals("fra", hetero.getActiveAz());
        Assert.assertEquals(Arrays.asList("fra", "fra-ali"), hetero.getAzs());
    }

    @Test
    public void testGetClustersDefaultPreferRegionIsSha() {
        ClusterDTO hetero = findHeteroDualOneWayCluster(clusterService.getClusters(ClusterType.HETERO.name()));
        Assert.assertNotNull(hetero);
        Assert.assertEquals("jq", hetero.getActiveAz());
        Assert.assertEquals(Arrays.asList("jq", "oy"), hetero.getAzs());
    }

    @Test
    public void testOneWayClusterIgnoresPreferRegion() {
        ClusterDTO oneWay = clusterService.getClusters(ClusterType.ONE_WAY.name(), "FRA").stream()
                .filter(cluster -> "cluster1".equals(cluster.getClusterName()))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(oneWay);
        Assert.assertEquals("jq", oneWay.getActiveAz());
        Assert.assertTrue(oneWay.getAzs().contains("oy"));
        Assert.assertTrue(oneWay.getAzs().contains("fra"));
    }

    @Test
    public void testFindClusterDcGroupsForHeteroDualOneWay() {
        List<ClusterDcGroupModel> groups = clusterService.findClusterDcGroups("hetero-dual-oneway");
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals("FRA", groups.get(0).getRegion());
        Assert.assertEquals(ClusterType.ONE_WAY.name(), groups.get(0).getAzGroupClusterType());
        Assert.assertEquals(Arrays.asList("fra", "fra-ali"),
                groups.get(0).getDcs().stream().map(dc -> dc.getDcName()).collect(Collectors.toList()));
        Assert.assertEquals("SHA", groups.get(1).getRegion());
        Assert.assertEquals(Arrays.asList("jq", "oy"),
                groups.get(1).getDcs().stream().map(dc -> dc.getDcName()).collect(Collectors.toList()));
    }

    @Test
    public void testFindClusterDcGroupsReturnsEmptyForOneWayCluster() {
        Assert.assertTrue(clusterService.findClusterDcGroups("cluster1").isEmpty());
    }

    @Test
    public void testGetClustersAcceptsLowerCaseClusterType() {
        List<ClusterDTO> hetero = clusterService.getClusters("hetero", "SHA");
        Assert.assertFalse(hetero.isEmpty());
        Assert.assertNotNull(findHeteroDualOneWayCluster(hetero));
    }

    private ClusterDTO findHeteroDualOneWayCluster(List<ClusterDTO> clusters) {
        return clusters.stream()
                .filter(cluster -> "hetero-dual-oneway".equals(cluster.getClusterName()))
                .findFirst()
                .orElse(null);
    }
}
