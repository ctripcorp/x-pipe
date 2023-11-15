package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterExchangeNameInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterRegionExchangeInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionInfo;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterDeleteEventFactory;
import com.ctrip.xpipe.redis.console.service.impl.ClusterServiceImpl;
import com.ctrip.xpipe.redis.console.service.impl.ShardServiceImpl;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.when;


public class ClusterUpdateControllerTest extends AbstractConsoleIntegrationTest {
    @Autowired
    private ClusterUpdateController clusterController;
    @Autowired
    private ClusterServiceImpl clusterService;
    @Autowired
    private ClusterDao clusterDao;
    @Autowired
    private ShardServiceImpl shardService;

    @Mock
    private ConsoleConfig config;
    @Mock
    private ClusterDeleteEventFactory clusterDeleteEventFactory;

    private final String clusterName = "cluster-name";
    private final String clusterType = "ONE_WAY";
    private final List<String> azs1 = Arrays.asList("jq", "oy");
    private final List<String> azs2 = Arrays.asList("jq", "oy", "fra");
    private final String desc = "desc";
    private final long orgId = 0L;
    private final String email = "a@trip.com";
    private final RegionInfo region1 = new RegionInfo("SHA", "ONE_WAY", "jq", Arrays.asList("jq", "oy"));
    private final RegionInfo region2 = new RegionInfo("FRA", "SINGLE_DC", "fra", Collections.singletonList("fra"));
    private final List<RegionInfo> regions = Arrays.asList(region1, region2);

    private ClusterCreateInfo createInfo;

    @Before
    public void setup() {
        ClusterCreateInfo createInfo = new ClusterCreateInfo();
        createInfo.setClusterName(clusterName);
        createInfo.setClusterType(clusterType);
        createInfo.setDcs(azs1);
        createInfo.setDesc(desc);
        createInfo.setOrganizationId(orgId);
        createInfo.setClusterAdminEmails(email);
        this.createInfo = createInfo;
    }

    private void createCluster(String clusterName, String clusterType, List<String> azs, List<RegionInfo> regions) {
        if (!StringUtil.isEmpty(clusterName)) {
            createInfo.setClusterName(clusterName);
        }
        if (!StringUtil.isEmpty(clusterType)) {
            createInfo.setClusterType(clusterType);
        }
        if (!CollectionUtils.isEmpty(azs)) {
            createInfo.setDcs(azs);
        }
        if (!CollectionUtils.isEmpty(regions)) {
            createInfo.setRegions(regions);
        }
        clusterController.createCluster(createInfo);
    }


    @Test
    public void testCreateSingleGroupCluster() {
        this.createCluster(null, null, null, null);

        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        assertClusterEquals(cluster, "ONE_WAY");
        Assert.assertEquals(Arrays.asList("jq", "oy"), cluster.getDcs());
    }

    @Test
    public void testCreateMultiGroupCluster() {
        this.createCluster(null, null, azs2, regions);

        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        assertClusterEquals(cluster, "ONE_WAY");
        Assert.assertEquals(Arrays.asList("jq", "oy", "fra"), cluster.getDcs());
        Assert.assertEquals(regions, cluster.getRegions());
    }

    private void assertClusterEquals(ClusterCreateInfo cluster, String expectType) {
        Assert.assertEquals("cluster-name", cluster.getClusterName());
        Assert.assertEquals(expectType, cluster.getClusterType());
        Assert.assertEquals("desc", cluster.getDesc());
        Assert.assertEquals(0L, cluster.getOrganizationId().longValue());
        Assert.assertEquals("a@trip.com", cluster.getClusterAdminEmails());
    }

    @Test
    public void testDeleteCluster() {
        ClusterTbl clusterTbl1 = new ClusterTbl()
            .setClusterName("cluster-name1")
            .setClusterType(ClusterType.ONE_WAY.toString())
            .setClusterDescription("")
            .setActivedcId(1)
            .setIsXpipeInterested(true)
            .setStatus("normal")
            .setClusterDesignatedRouteIds("")
            .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        ClusterTbl clusterTbl2 = new ClusterTbl()
            .setClusterName("cluster-name2")
            .setClusterType(ClusterType.SINGLE_DC.toString())
            .setClusterDescription("")
            .setActivedcId(2)
            .setIsXpipeInterested(true)
            .setStatus("normal")
            .setClusterDesignatedRouteIds("")
            .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        clusterDao.createCluster(clusterTbl1);
        clusterDao.createCluster(clusterTbl2);

        clusterService.setClusterDeleteEventFactory(clusterDeleteEventFactory);
        clusterService.setConsoleConfig(config);
        when(clusterDeleteEventFactory.createClusterEvent(Mockito.anyString(), Mockito.any())).thenReturn(null);
        when(config.getOwnClusterType()).thenReturn(Collections.emptySet());

        clusterController.deleteCluster("cluster-name1", false);
        List<ClusterTbl> allClusters = clusterDao.findAllClusters();
        Assert.assertEquals(1, allClusters.size());
        ClusterTbl clusterTbl = allClusters.get(0);
        Assert.assertEquals("cluster-name2", clusterTbl.getClusterName());
    }

    @Test
    public void testGetClusters() throws CheckFailException {
        ClusterTbl clusterTbl1 = new ClusterTbl()
            .setClusterName("cluster-name1")
            .setClusterType(ClusterType.SINGLE_DC.toString())
            .setClusterDescription("")
            .setActivedcId(1)
            .setIsXpipeInterested(true)
            .setStatus("normal")
            .setClusterDesignatedRouteIds("")
            .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        ClusterTbl clusterTbl2 = new ClusterTbl()
            .setClusterName("cluster-name2")
            .setClusterType(ClusterType.SINGLE_DC.toString())
            .setClusterDescription("")
            .setActivedcId(2)
            .setIsXpipeInterested(true)
            .setStatus("normal")
            .setClusterDesignatedRouteIds("")
            .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        clusterDao.createCluster(clusterTbl1);
        clusterDao.createCluster(clusterTbl2);

        List<ClusterCreateInfo> clusters = clusterController.getClusters(ClusterType.SINGLE_DC.toString());
        Assert.assertEquals(2, clusters.size());
        Assert.assertEquals("cluster-name1", clusters.get(0).getClusterName());
        Assert.assertEquals("cluster-name2", clusters.get(1).getClusterName());
    }

    @Test
    public void updateCluster() {
        String clusterName = "cluster-name";

        ClusterTbl clusterTbl = new ClusterTbl()
            .setClusterName(clusterName)
            .setClusterType(ClusterType.ONE_WAY.toString())
            .setClusterDescription("")
            .setActivedcId(1)
            .setIsXpipeInterested(true)
            .setStatus("normal")
            .setClusterDesignatedRouteIds("")
            .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        clusterDao.createCluster(clusterTbl);

        long orgId = 5L;
        String desc = "update desc", adminEmails = "test@ctrip.com";
        ClusterCreateInfo clusterInfo = new ClusterCreateInfo();
        clusterInfo.setClusterName(clusterName);
        clusterInfo.setClusterType(ClusterType.HETERO.toString());
        clusterInfo.setDesc(desc);

        RetMessage retMessage = clusterController.updateCluster(clusterInfo);
        logger.info("{}", retMessage.getMessage());
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());

        ClusterTbl cluster = clusterDao.findClusterAndOrgByName(clusterName);
        Assert.assertEquals(ClusterType.HETERO.toString(), cluster.getClusterType());
        // 目前api接口不更新desc
        Assert.assertEquals("", cluster.getClusterDescription());
        Assert.assertEquals(0L, cluster.getClusterOrgId());
        Assert.assertNull(cluster.getClusterAdminEmails());

        clusterInfo.setOrganizationId(orgId);
        clusterController.updateCluster(clusterInfo);
        cluster = clusterDao.findClusterAndOrgByName(clusterName);
        Assert.assertEquals(orgId, cluster.getOrganizationInfo().getOrgId());
        Assert.assertNull(cluster.getClusterAdminEmails());

        clusterInfo.setClusterAdminEmails(adminEmails);
        clusterController.updateCluster(clusterInfo);
        cluster = clusterDao.findClusterAndOrgByName(clusterName);
        Assert.assertEquals(adminEmails, cluster.getClusterAdminEmails());
    }

    @Test
    public void testUpdateClusterWithNoClusterFound() {
        String CLUSTER_NAME = "cluster-not-exist";
        String EXPECTED_MESSAGE = String.format("Not find cluster: %s", CLUSTER_NAME);
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterName(CLUSTER_NAME);
        clusterCreateInfo.setOrganizationId(0L);

        RetMessage retMessage = clusterController.updateClusters(Collections.singletonList(clusterCreateInfo));
        logger.info("{}", retMessage.getMessage());
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());
        Assert.assertEquals(EXPECTED_MESSAGE, retMessage.getMessage());
    }

    @Test
    public void testUpdateClusterWithNoNeedUpdate() {
        String CLUSTER_NAME = "cluster-name";
        String EXPECTED_MESSAGE = String.format("No field changes for cluster: %s", CLUSTER_NAME);
        long ORG_ID = 5L;
        ClusterTbl clusterTbl = new ClusterTbl().setClusterName(CLUSTER_NAME)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterDescription("")
                .setActivedcId(1)
                .setIsXpipeInterested(true)
                .setStatus("normal")
                .setClusterDesignatedRouteIds("")
                .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        clusterDao.createCluster(clusterTbl);

        ClusterCreateInfo clusterInfo = new ClusterCreateInfo();
        clusterInfo.setClusterName(CLUSTER_NAME);
        clusterInfo.setClusterAdminEmails("test@ctrip.com");
        clusterInfo.setOrganizationId(ORG_ID);
        RetMessage retMessage = clusterController.updateCluster(clusterInfo);
        logger.info("{}", retMessage.getMessage());

        RetMessage retMessage1 = clusterController.updateCluster(clusterInfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage1.getState());
        Assert.assertEquals(EXPECTED_MESSAGE, retMessage1.getMessage());

        ClusterTbl cluster = clusterDao.findClusterAndOrgByName(CLUSTER_NAME);
        Assert.assertEquals(ORG_ID, cluster.getOrganizationInfo().getOrgId());
    }

    @Test
    public void testUpdateClusterWithNoOrgIDFound() throws Exception {
        String CLUSTER_NAME = "cluster-name";
        long ORG_ID = 99L;
        String EXPECTED_MESSAGE = String.format("Organization Id: %d, could not be found", ORG_ID);
        ClusterTbl clusterTbl = new ClusterTbl().setClusterName(CLUSTER_NAME)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterDescription("")
                .setActivedcId(1)
                .setIsXpipeInterested(true)
                .setStatus("normal")
                .setClusterDesignatedRouteIds("")
                .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        clusterDao.createCluster(clusterTbl);

        ClusterCreateInfo clusterInfo = new ClusterCreateInfo();
        clusterInfo.setClusterName(CLUSTER_NAME);
        clusterInfo.setClusterAdminEmails("test@ctrip.com");
        clusterInfo.setOrganizationId(ORG_ID);
        RetMessage retMessage = clusterController.updateCluster(clusterInfo);
        logger.info("{}", retMessage.getMessage());

        RetMessage retMessage1 = clusterController.updateCluster(clusterInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage1.getState());
        Assert.assertEquals(EXPECTED_MESSAGE, retMessage1.getMessage());
    }

    @Test
    public void testClusterExchangeName() throws Exception{
        clusterController.setConfig(config);
        when(config.getOuterClientToken()).thenReturn("xxxx");
        String FORMER_NAME = "cluster101", LATTER_NAME="cluster102";
        Long FORMER_ID = 101L, LATTER_ID = 102L;
        ClusterTbl clusterTbl = new ClusterTbl()
                .setId(FORMER_ID)
                .setClusterName(FORMER_NAME)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterDescription("")
                .setActivedcId(1)
                .setIsXpipeInterested(true)
                .setStatus("normal")
                .setClusterDesignatedRouteIds("")
                .setClusterLastModifiedTime(DateTimeUtils.currentTimeAsString());
        clusterDao.createCluster(clusterTbl);
        clusterTbl.setId(LATTER_ID).setClusterName(LATTER_NAME);
        clusterDao.createCluster(clusterTbl);

        /* fail on param check. */
        RetMessage retMessage;
        ClusterExchangeNameInfo exinfo = new ClusterExchangeNameInfo();
        retMessage = clusterController.exchangeName(exinfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());

        /* suc on first exchange attempt */
        exinfo.setFormerClusterId(FORMER_ID);
        exinfo.setFormerClusterName(FORMER_NAME);
        exinfo.setLatterClusterId(LATTER_ID);
        exinfo.setLatterClusterName(LATTER_NAME);
        exinfo.setToken("xxxx");
        retMessage = clusterController.exchangeName(exinfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());

        /* fail on retry exchange attempt */
        retMessage = clusterController.exchangeName(exinfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());

        /* suc on exchage back */
        exinfo.setFormerClusterName(LATTER_NAME);
        exinfo.setLatterClusterName(FORMER_NAME);
        retMessage = clusterController.exchangeName(exinfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
    }

    @Test
    public void testExchangeRegion() {
        this.createCluster(null, null, null, null);
        ShardTbl shard = shardService.createShard("cluster-name", new ShardTbl().setShardName("shard1"), new HashMap<>());
        clusterController.upgradeAzGroup("cluster-name");

        this.createCluster("hetero-cluster", "HETERO", Arrays.asList("jq", "oy", "fra"), Arrays.asList(region1, region2));
        shardService.createRegionShard("hetero-cluster", "SHA", "hetero-shard1");

        ClusterRegionExchangeInfo info = new ClusterRegionExchangeInfo(1L, "cluster-name", 2L, "hetero-cluster", "SHA");
        RetMessage ret = clusterController.exchangeClusterRegion(info);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        ShardTbl heteroShard = shardService.findAllShardNamesByClusterName("hetero-cluster").get(0);
        Assert.assertEquals(shard.getShardName(), heteroShard.getShardName());
    }

    @Test
    public void testExchangeRegion2() {
        this.createCluster(null, "SINGLE_DC", Arrays.asList("jq", "fra"), null);
        clusterService.unbindDc("cluster-name", "jq");
        ShardTbl shard = shardService.createShard("cluster-name", new ShardTbl().setShardName("shard2"), new HashMap<>());
        clusterController.upgradeAzGroup("cluster-name");

        this.createCluster("hetero-cluster", "HETERO", Arrays.asList("jq", "oy", "fra"), Arrays.asList(region1, region2));
        shardService.createRegionShard("hetero-cluster", "FRA", "hetero-shard2");

        ClusterRegionExchangeInfo info = new ClusterRegionExchangeInfo(1L, "cluster-name", 2L, "hetero-cluster", "FRA");
        RetMessage ret = clusterController.exchangeClusterRegion(info);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        ShardTbl heteroShard = shardService.findAllShardNamesByClusterName("hetero-cluster").get(0);
        Assert.assertEquals(shard.getShardName(), heteroShard.getShardName());
    }

    @Test
    public void testUpgradeOneWayClusterToAzGroup() {
        this.createCluster(null, null, null, null);

        clusterController.upgradeAzGroup(clusterName);
        ClusterCreateInfo cluster = clusterController.getCluster(clusterName);
        Assert.assertEquals(clusterType, cluster.getClusterType());
        Assert.assertEquals(1, cluster.getRegions().size());
        RegionInfo region = cluster.getRegions().get(0);
        Assert.assertEquals(clusterType, region.getClusterType());
        Assert.assertEquals("jq", region.getActiveAz());
        Assert.assertEquals(azs1, region.getAzs());
    }

    @Test
    public void testUpgradeBiClusterToAzGroup() {
        this.createCluster(null, ClusterType.BI_DIRECTION.toString(), null, null);

        clusterController.upgradeAzGroup("cluster-name");
        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals("BI_DIRECTION", cluster.getClusterType());
        Assert.assertEquals(2, cluster.getRegions().size());

        RegionInfo region1 = cluster.getRegions().get(0);
        Assert.assertEquals("", region1.getClusterType());
        Assert.assertEquals("jq", region1.getActiveAz());
        Assert.assertEquals(Collections.singletonList("jq"), region1.getAzs());

        RegionInfo region2 = cluster.getRegions().get(1);
        Assert.assertEquals("", region2.getClusterType());
        Assert.assertEquals("oy", region2.getActiveAz());
        Assert.assertEquals(Collections.singletonList("oy"), region2.getAzs());
    }

    @Test
    public void testDowngradeAzGroupToOnewayCluster() {
        this.createCluster(null, "HETERO", Arrays.asList("jq", "oy", "fra"), Arrays.asList(region1, region2));
        clusterController.unbindDc("cluster-name", "fra");

        clusterController.downgradeAzGroup("cluster-name");
        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals(0, cluster.getRegions().size());
        Assert.assertEquals(Arrays.asList("jq", "oy"), cluster.getDcs());
    }

    @Test
    public void testBindDc() {
        this.createCluster(null, null, null, null);

        RetMessage ret = clusterController.bindDc("cluster-name", "fra", null);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());
        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals(3, cluster.getDcs().size());
        Assert.assertEquals("fra", cluster.getDcs().get(2));
    }

    @Test
    public void testBindDuplicatedDc() {
        this.createCluster(null, null, null, null);

        RetMessage ret = clusterController.bindDc("cluster-name", "jq", null);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs("cluster-name");
        Assert.assertEquals(2, dcTbls.size());
    }

    @Test
    public void testBindRegionAz() {
        this.createCluster(null, null, null, null);
        clusterController.upgradeAzGroup("cluster-name");

        clusterController.bindRegionAz("cluster-name", "FRA", "fra");
        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals(2, cluster.getRegions().size());

        RegionInfo region1 = cluster.getRegions().get(0);
        Assert.assertEquals("ONE_WAY", region1.getClusterType());
        Assert.assertEquals("jq", region1.getActiveAz());
        Assert.assertEquals(Arrays.asList("jq", "oy"), region1.getAzs());

        RegionInfo region2 = cluster.getRegions().get(1);
        Assert.assertEquals("SINGLE_DC", region2.getClusterType());
        Assert.assertEquals("fra", region2.getActiveAz());
        Assert.assertEquals(Collections.singletonList("fra"), region2.getAzs());
    }

    @Test
    public void testBindRegionAz2() {
        this.createCluster(null, null, null, null);
        clusterController.upgradeAzGroup("cluster-name");
        clusterController.unbindDc("cluster-name", "oy");

        clusterController.bindRegionAz("cluster-name", "SHA", "oy");
        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals(1, cluster.getRegions().size());

        RegionInfo region = cluster.getRegions().get(0);
        Assert.assertEquals("ONE_WAY", region.getClusterType());
        Assert.assertEquals("jq", region.getActiveAz());
        Assert.assertEquals(Arrays.asList("jq", "oy"), region.getAzs());
    }

    @Test
    public void testUnbindActiveDc() {
        this.createCluster(null, null, null, null);

        RetMessage ret = clusterController.unbindDc("cluster-name", "jq");
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs("cluster-name");
        Assert.assertEquals(2, dcTbls.size());
        Assert.assertEquals("jq", dcTbls.get(0).getDcName());
    }

    @Test
    public void testUnbindDc() {
        this.createCluster(null, null, azs2, regions);

        clusterController.unbindDc("cluster-name", "oy");
        ClusterCreateInfo cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals(2, cluster.getDcs().size());
        Assert.assertEquals("jq", cluster.getDcs().get(0));
        Assert.assertEquals("fra", cluster.getDcs().get(1));

        Assert.assertEquals(2, cluster.getRegions().size());
        RegionInfo region = cluster.getRegions().stream().filter(r -> r.getRegion().equals("FRA")).findFirst().get();
        Assert.assertEquals("SINGLE_DC", region.getClusterType());
        Assert.assertEquals("fra", region.getActiveAz());
        Assert.assertEquals(Collections.singletonList("fra"), region.getAzs());

        clusterController.unbindDc("cluster-name", "fra");
        cluster = clusterController.getCluster("cluster-name");
        Assert.assertEquals(1, cluster.getDcs().size());
        Assert.assertEquals("jq", cluster.getDcs().get(0));

        Assert.assertEquals(1, cluster.getRegions().size());
        region = cluster.getRegions().get(0);
        Assert.assertEquals("ONE_WAY", region.getClusterType());
        Assert.assertEquals("jq", region.getActiveAz());
        Assert.assertEquals(Collections.singletonList("jq"), region.getAzs());
    }

    @Test
    public void testUnbindNotEmptyDc() {
//        List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
//            Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));
//        shardCont.c(clusterName, shardName, createInfo);
//
//        RetMessage ret = metaUpdate.unbindDc(clusterName, backupDC);
//        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
//
//        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
//        Assert.assertEquals(2, dcTbls.size());
    }

}