package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.impl.AzGroupCacheImpl;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterUpdateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionInfo;
import com.ctrip.xpipe.redis.console.dto.ClusterDTO;
import com.ctrip.xpipe.redis.console.service.impl.ClusterServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Phase C: cross-region AzGroup ops API — create/bind, wrong region, D5 containedRegions disjoint, preferRegion, regions field.
 */
public class ClusterUpdateControllerCrossRegionAzGroupTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterUpdateController clusterController;

    @Autowired
    private ClusterServiceImpl clusterService;

    @Autowired
    private AzGroupCache azGroupCache;

    @Before
    public void refreshAzGroupCache() throws Exception {
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

    @Test
    public void testCreateCrossRegionOneWayAndReadRegions() {
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-create");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy", "fra", "sgp"));
        RegionInfo cross = new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy", "fra"));
        RegionInfo sgp = new RegionInfo("SGP", ClusterType.SINGLE_DC.name(), "sgp", Collections.singletonList("sgp"));
        createInfo.setRegions(Arrays.asList(cross, sgp));

        RetMessage ret = clusterController.createCluster(createInfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        ClusterCreateInfo cluster = clusterController.getCluster("cross-region-create");
        Assert.assertEquals(2, cluster.getRegions().size());

        RegionInfo sha = cluster.getRegions().stream().filter(r -> "SHA".equals(r.getRegion())).findFirst().get();
        Assert.assertEquals("jq", sha.getActiveAz());
        Assert.assertEquals(new HashSet<>(Arrays.asList("jq", "oy", "fra")), new HashSet<>(sha.getAzs()));
        Assert.assertEquals(new HashSet<>(Arrays.asList("SHA", "FRA")), new HashSet<>(sha.getRegions()));

        RegionInfo sgpRead = cluster.getRegions().stream().filter(r -> "SGP".equals(r.getRegion())).findFirst().get();
        Assert.assertEquals(Collections.singletonList("sgp"), sgpRead.getAzs());
        Assert.assertEquals(Collections.singletonList("SGP"), sgpRead.getRegions());
    }

    @Test
    public void testCreateRejectsRegionNotMatchingActiveAz() {
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-wrong-region");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy", "fra"));
        // region FRA but activeAz jq is in SHA
        createInfo.setRegions(Collections.singletonList(
                new RegionInfo("FRA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy", "fra"))));

        RetMessage ret = clusterController.createCluster(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("doesn't match active az region"));
    }

    @Test
    public void testCreateRejectsEmptyRegion() {
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-empty-region");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy"));
        createInfo.setRegions(Collections.singletonList(
                new RegionInfo("", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy"))));

        RetMessage ret = clusterController.createCluster(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("region is empty"));
    }

    @Test
    public void testCreateRejectsDuplicateActiveRegion() {
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-dup-active");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy", "fra"));
        // CROSS_SHA_FRA + LOCAL_SHA：containedRegions 在 SHA 重叠（亦撞 activeRegion）
        createInfo.setRegions(Arrays.asList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy", "fra")),
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "oy", Arrays.asList("jq", "oy"))));

        RetMessage ret = clusterController.createCluster(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("covering region"));
    }

    @Test
    public void testCreateRejectsOverlappingContainedRegion() {
        // CROSS active=SHA 与 LOCAL_FRA active=FRA：activeRegion 不撞，但 FRA ∈ both containedRegions
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-dup-contained");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy", "fra"));
        createInfo.setRegions(Arrays.asList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy", "fra")),
                new RegionInfo("FRA", ClusterType.SINGLE_DC.name(), "fra", Collections.singletonList("fra"))));

        RetMessage ret = clusterController.createCluster(createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("covering region"));
    }

    @Test
    public void testBindCrossRegionAzIntoExistingActiveRegion() {
        // Fixture hetero-cross-region has redises in fra → unbindDc fails "cluster not empty".
        // Create empty SHA ONE_WAY (jq/oy), then bind fra into existing activeRegion SHA → CROSS_SHA_FRA.
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-bind-existing");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy"));
        createInfo.setRegions(Collections.singletonList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy"))));
        Assert.assertEquals(RetMessage.SUCCESS_STATE, clusterController.createCluster(createInfo).getState());

        RetMessage bind = clusterController.bindRegionAz("cross-region-bind-existing", "SHA", "fra");
        Assert.assertEquals(RetMessage.SUCCESS_STATE, bind.getState());

        ClusterCreateInfo cluster = clusterController.getCluster("cross-region-bind-existing");
        RegionInfo sha = cluster.getRegions().stream().filter(r -> "SHA".equals(r.getRegion())).findFirst().get();
        Assert.assertEquals(new HashSet<>(Arrays.asList("jq", "oy", "fra")), new HashSet<>(sha.getAzs()));
        Assert.assertEquals(new HashSet<>(Arrays.asList("SHA", "FRA")), new HashSet<>(sha.getRegions()));
    }

    @Test
    public void testBindRejectsWrongRegionNameWhenCreatingNewGroup() {
        // Create empty cluster (no redis) so bind goes to "create new AzGroupCluster" path.
        // Fixture hetero-cross-region has redises in fra → unbindDc would fail "cluster not empty".
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-bind-wrong");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy"));
        createInfo.setRegions(Collections.singletonList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy"))));
        Assert.assertEquals(RetMessage.SUCCESS_STATE, clusterController.createCluster(createInfo).getState());

        RetMessage ret = clusterController.bindRegionAz("cross-region-bind-wrong", "XXX", "fra");
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("doesn't match az region"));
    }

    @Test
    public void testGetClustersPreferRegionCrossRegionOneWay() {
        ClusterDTO sha = findCross(clusterService.getClusters(ClusterType.HETERO.name(), "SHA"));
        Assert.assertNotNull(sha);
        Assert.assertEquals("jq", sha.getActiveAz());
        Assert.assertTrue(sha.getAzs().contains("fra"));

        // FRA is contained but not activeRegion → no match, fall back to the only ONE_WAY (SHA)
        ClusterDTO fraPrefer = findCross(clusterService.getClusters(ClusterType.HETERO.name(), "FRA"));
        Assert.assertNotNull(fraPrefer);
        Assert.assertEquals("jq", fraPrefer.getActiveAz());
    }

    @Test
    public void testUpdateActiveAzWithinSameRegion() {
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-update-az");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy", "fra"));
        createInfo.setRegions(Collections.singletonList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy", "fra"))));
        Assert.assertEquals(RetMessage.SUCCESS_STATE, clusterController.createCluster(createInfo).getState());

        ClusterUpdateInfo update = new ClusterUpdateInfo();
        update.setClusterName("cross-region-update-az");
        update.setRegions(Collections.singletonList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "oy", Arrays.asList("jq", "oy", "fra"))));
        RetMessage ret = clusterController.updateCluster(update);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        ClusterCreateInfo cluster = clusterController.getCluster("cross-region-update-az");
        RegionInfo sha = cluster.getRegions().stream().filter(r -> "SHA".equals(r.getRegion())).findFirst().get();
        Assert.assertEquals("oy", sha.getActiveAz());
    }

    @Test
    public void testUpdateRejectsActiveAzNotInAzGroup() {
        ClusterCreateInfo createInfo = baseCreateInfo("cross-region-update-bad-az");
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDcs(Arrays.asList("jq", "oy", "sgp"));
        createInfo.setRegions(Arrays.asList(
                new RegionInfo("SHA", ClusterType.ONE_WAY.name(), "jq", Arrays.asList("jq", "oy")),
                new RegionInfo("SGP", ClusterType.SINGLE_DC.name(), "sgp", Collections.singletonList("sgp"))));
        Assert.assertEquals(RetMessage.SUCCESS_STATE, clusterController.createCluster(createInfo).getState());

        ClusterUpdateInfo update = new ClusterUpdateInfo();
        update.setClusterName("cross-region-update-bad-az");
        update.setRegions(Collections.singletonList(
                new RegionInfo("SGP", ClusterType.SINGLE_DC.name(), "jq", Collections.singletonList("sgp"))));
        RetMessage ret = clusterController.updateCluster(update);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
        Assert.assertTrue(ret.getMessage(), ret.getMessage().contains("not in azs"));
    }

    private ClusterCreateInfo baseCreateInfo(String name) {
        ClusterCreateInfo createInfo = new ClusterCreateInfo();
        createInfo.setClusterName(name);
        createInfo.setClusterType(ClusterType.HETERO.name());
        createInfo.setDesc("cross-region phase-c");
        createInfo.setOrganizationId(0L);
        createInfo.setClusterAdminEmails("a@trip.com");
        return createInfo;
    }

    private ClusterDTO findCross(List<ClusterDTO> clusters) {
        return clusters.stream()
                .filter(c -> "hetero-cross-region".equals(c.getClusterName()))
                .findFirst()
                .orElse(null);
    }
}
