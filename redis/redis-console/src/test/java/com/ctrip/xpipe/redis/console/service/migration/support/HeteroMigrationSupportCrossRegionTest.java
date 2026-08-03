package com.ctrip.xpipe.redis.console.service.migration.support;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.impl.AzGroupCacheImpl;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Phase B: cross-region ONE_WAY AzGroup migration helpers (activeRegion pick/sort + sourceDc∈azs resolve).
 */
public class HeteroMigrationSupportCrossRegionTest extends AbstractConsoleIntegrationTest {

    private static final String CLUSTER = "hetero-cross-region";

    @Autowired
    private HeteroMigrationSupport heteroMigrationSupport;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private AzGroupCache azGroupCache;

    private long clusterId;
    private AzGroupClusterEntity crossOneWay;

    @Before
    public void prepare() throws Exception {
        refreshAzGroupCache();
        ClusterTbl cluster = clusterService.find(CLUSTER);
        Assert.assertNotNull(cluster);
        clusterId = cluster.getId();
        crossOneWay = heteroMigrationSupport.listOneWayAzGroupClustersSorted(clusterId).stream()
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(crossOneWay);
        Assert.assertEquals("SHA", heteroMigrationSupport.activeRegion(crossOneWay));
    }

    @Test
    public void pickAndSortShouldUseActiveRegionNotContainedRegions() {
        AzGroupClusterEntity pickedSha = heteroMigrationSupport.pickOneWayAzGroupClusterByRegion(clusterId, "SHA");
        Assert.assertNotNull(pickedSha);
        Assert.assertEquals(crossOneWay.getId(), pickedSha.getId());

        // FRA is contained in AzGroup but not activeRegion → no match, fall back to sorted first (SHA)
        AzGroupClusterEntity pickedFra = heteroMigrationSupport.pickOneWayAzGroupClusterByRegion(clusterId, "FRA");
        Assert.assertNotNull(pickedFra);
        Assert.assertEquals(crossOneWay.getId(), pickedFra.getId());

        List<AzGroupClusterEntity> display = new ArrayList<>(
                heteroMigrationSupport.listDisplayAzGroupClustersSorted(java.util.Collections.singletonList(clusterId))
                        .get(clusterId));
        Assert.assertTrue(display.size() >= 2);
        // Sorted by activeRegion string: SGP < SHA
        Assert.assertEquals("SGP", heteroMigrationSupport.activeRegion(display.get(0)));
        Assert.assertEquals("SHA", heteroMigrationSupport.activeRegion(display.get(1)));
    }

    @Test
    public void resolveBySourceDcShouldMatchAnyAzInCrossRegionAzGroup() {
        Map<Long, AzGroupClusterEntity> byJq = heteroMigrationSupport.resolveMigrationAzGroupClusters(
                java.util.Collections.singletonList(clusterId), "jq");
        Map<Long, AzGroupClusterEntity> byOy = heteroMigrationSupport.resolveMigrationAzGroupClusters(
                java.util.Collections.singletonList(clusterId), "oy");
        Map<Long, AzGroupClusterEntity> byFra = heteroMigrationSupport.resolveMigrationAzGroupClusters(
                java.util.Collections.singletonList(clusterId), "fra");

        Assert.assertEquals(crossOneWay.getId(), byJq.get(clusterId).getId());
        Assert.assertEquals(crossOneWay.getId(), byOy.get(clusterId).getId());
        Assert.assertEquals(crossOneWay.getId(), byFra.get(clusterId).getId());

        // SINGLE_DC overseas is skipped by migration-only resolve
        Assert.assertFalse(heteroMigrationSupport.resolveMigrationAzGroupClusters(
                java.util.Collections.singletonList(clusterId), "sgp").containsKey(clusterId));
    }

    @Test
    public void isSameAzGroupShouldAllowCrossRegionTargetInsideAzGroup() {
        Assert.assertTrue(heteroMigrationSupport.isSameAzGroup(clusterId, "jq", "fra"));
        Assert.assertTrue(heteroMigrationSupport.isSameAzGroup(clusterId, "jq", "oy"));
        Assert.assertFalse(heteroMigrationSupport.isSameAzGroup(clusterId, "jq", "sgp"));
    }

    private void refreshAzGroupCache() throws Exception {
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
        return prepareDatasFromFile("src/test/resources/hetero-cross-region-az-group-test.sql");
    }
}
