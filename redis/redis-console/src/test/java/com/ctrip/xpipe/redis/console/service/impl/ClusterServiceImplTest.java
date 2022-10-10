package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.model.consoleportal.ProxyChainModel;
import com.ctrip.xpipe.redis.console.model.consoleportal.RouteInfoModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.TunnelInfo;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultProxyChain;
import com.ctrip.xpipe.redis.console.proxy.impl.DefaultTunnelInfo;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.core.entity.Route;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 20, 2017
 */
public class ClusterServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    private ClusterServiceImpl clusterService;

    @Autowired
    private OrganizationService organizationService;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardServiceImpl dcClusterShardService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ClusterDao clusterDao;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private MigrationEventDao migrationEventDao;

    @Autowired
    private RouteService routeService;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ApplierService applierService;

    @Autowired
    private ReplDirectionService replDirectionService;

    @Test
    public void testCreateOneWayCluster(){

        String clusterName = randomString(10);
        List<DcTbl> dcTbls = dcService.findAllDcs();

        ClusterModel clusterModel = new ClusterModel();

        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(1)
                .setClusterName(clusterName)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
        );
        List<DcClusterModel> dcClusters = new LinkedList<>();
        dcTbls.forEach(dcTbl -> {
            DcModel dcModel = new DcModel();
            dcModel.setDc_name(dcTbl.getDcName());
            dcClusters.add(new DcClusterModel().setDc(dcModel).setDcCluster(new DcClusterTbl()));
        });
        clusterModel.setDcClusters(dcClusters);
        clusterService.createCluster(clusterModel);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName)));

        //test clusterDesignatedRoute
        String clusterName2 = randomString(10);
        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(1)
                .setClusterName(clusterName2)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
                .setClusterDesignatedRouteIds("1,2")
        );
        List<DcClusterModel> dcClusters1 = new LinkedList<>();
        dcTbls.forEach(dcTbl -> {
            DcModel dcModel = new DcModel();
            dcModel.setDc_name(dcTbl.getDcName());
            dcClusters1.add(new DcClusterModel().setDc(dcModel).setDcCluster(new DcClusterTbl()));
        });
        clusterModel.setDcClusters(dcClusters1);
        clusterService.createCluster(clusterModel);
        clusterTbl = clusterService.find(clusterName2);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName2)));

    }

    @Test
    public void testCreateHeteroCluster() {
        String clusterName = randomString(10);
        List<DcTbl> dcTbls = dcService.findAllDcs();

        ClusterModel clusterModel = new ClusterModel();

        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(1)
                .setClusterName(clusterName)
                // TODO: 2022/10/10 remove hetero
//                .setClusterType(ClusterType.HETERO.toString())
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterAdminEmails("test@1111.com")
                .setClusterDescription(randomString(20))
        );


        ShardModel shard1 = new ShardModel();
        shard1.setShardTbl(new ShardTbl().setShardName(clusterName + "_1").setSetinelMonitorName(clusterName + "_1"));
        ShardModel shard2 = new ShardModel();
        shard2.setShardTbl(new ShardTbl().setShardName(clusterName + "_2").setSetinelMonitorName(clusterName + "_2"));
        ShardModel shard3 = new ShardModel();
        shard3.setShardTbl(new ShardTbl().setShardName(clusterName + "_3").setSetinelMonitorName(clusterName + "_3"));
        ShardModel shard4 = new ShardModel();
        shard4.setShardTbl(new ShardTbl().setShardName(clusterName + "_4").setSetinelMonitorName(clusterName + "_4"));
        ShardModel shard5 = new ShardModel();
        shard5.setShardTbl(new ShardTbl().setShardName(clusterName + "_5").setSetinelMonitorName(clusterName + "_5"));

        DcModel jq = new DcModel();
        jq.setDc_name("jq");
        DcClusterModel jqDcCluster = new DcClusterModel().setDc(jq)
                                            .setDcCluster(new DcClusterTbl().setGroupName("jq").setGroupType(DcGroupType.DR_MASTER.toString()))
                                            .setShards(Lists.newArrayList(shard1, shard2, shard3));
        DcModel oy = new DcModel();
        oy.setDc_name("oy");
        DcClusterModel oyDcCluster = new DcClusterModel().setDc(oy)
                .setDcCluster(new DcClusterTbl().setGroupName("oy").setGroupType(DcGroupType.DR_MASTER.toString()))
                .setShards(Lists.newArrayList(shard1, shard2, shard3));

        DcModel fra = new DcModel();
        fra.setDc_name("fra");
        DcClusterModel fraDcCluster = new DcClusterModel().setDc(fra)
                .setDcCluster(new DcClusterTbl().setGroupName("fra").setGroupType(DcGroupType.MASTER.toString()))
                .setShards(Lists.newArrayList(shard4, shard5));
        clusterModel.setDcClusters(Lists.newArrayList(jqDcCluster, oyDcCluster, fraDcCluster));

        ReplDirectionInfoModel replDirectionInfoModel1 = new ReplDirectionInfoModel().setClusterName(clusterName)
                                                            .setSrcDcName("jq").setFromDcName("jq").setToDcName("oy");
        ReplDirectionInfoModel replDirectionInfoModel2 = new ReplDirectionInfoModel().setClusterName(clusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("fra");
        clusterModel.setReplDirections(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        clusterService.createCluster(clusterModel);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> {
            DcClusterTbl dcClusterTbl = dcClusterService.find(dcTbl.getDcName(), clusterName);
            Assert.assertNotNull(dcClusterTbl);

        });
        DcClusterTbl dcClusterTbl = dcClusterService.find("jq", clusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(jqDcCluster.getShards().size(), dcClusterShardService.findAllByDcCluster(dcClusterTbl.getDcClusterId()).size());

        DcClusterTbl dcClusterTbl2 = dcClusterService.find("oy", clusterName);
        Assert.assertNotNull(dcClusterTbl2);
        Assert.assertEquals(oyDcCluster.getShards().size(), dcClusterShardService.findAllByDcCluster(dcClusterTbl2.getDcClusterId()).size());

        DcClusterTbl dcClusterTbl3 = dcClusterService.find("fra", clusterName);
        Assert.assertNotNull(dcClusterTbl3);
        Assert.assertEquals(fraDcCluster.getShards().size(), dcClusterShardService.findAllByDcCluster(dcClusterTbl3.getDcClusterId()).size());
    }

    @Test
    public void testDeleteHeteroCluster() {
        String heteroClusterName = "hetero-cluster";
        long heteroClusterId = 7;
        String shard1 = "hetero-cluster_1";
        String shard2 = "hetero-cluster_oy_1";
        String shard4 = "hetero-cluster_oy_2";
        String shard3 = "hetero-cluster_fra_1";
        String shard5 = "hetero-cluster_fra_2";

        clusterService.deleteCluster(heteroClusterName);

        ClusterTbl clusterTbl = clusterService.find(heteroClusterName);
        Assert.assertNull(clusterTbl);

        DcClusterTbl dcClusterTbl = dcClusterService.find("oy", heteroClusterName);
        Assert.assertNull(dcClusterTbl);
        dcClusterTbl = dcClusterService.find("fra", heteroClusterName);
        Assert.assertNull(dcClusterTbl);
        dcClusterTbl = dcClusterService.find("jq", heteroClusterName);
        Assert.assertNull(dcClusterTbl);

        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);


        //oy
        List<RedisTbl> redisTbls = redisService.findAllByDcClusterShard(53);
        Assert.assertEquals(0, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(54);
        Assert.assertEquals(0, redisTbls.size());
        //fra
        redisTbls = redisService.findAllByDcClusterShard(55);
        Assert.assertEquals(0, redisTbls.size());
        //jq
        redisTbls = redisService.findAllByDcClusterShard(51);
        Assert.assertEquals(0, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(52);
        Assert.assertEquals(0, redisTbls.size());

        List<ApplierTbl> applierTbls = applierService.findApplierTblByShardAndReplDirection(21 , 2);
        Assert.assertEquals(0, applierTbls.size());
        applierTbls = applierService.findApplierTblByShardAndReplDirection(22 , 2);
        Assert.assertEquals(0, applierTbls.size());

        ShardTbl shardTbl = shardService.find(23);
        Assert.assertEquals(true, shardTbl.isDeleted());
        shardTbl = shardService.find(22);
        Assert.assertEquals(true, shardTbl.isDeleted());
        shardTbl = shardService.find(21);
        Assert.assertEquals(true, shardTbl.isDeleted());


        List<ReplDirectionTbl> allReplications = replDirectionService.findAllReplDirectionTblsByCluster(heteroClusterId);
        Assert.assertEquals(0, allReplications.size());

    }

    @Test
    public void testCreateBiDirectionCluster() {
        String clusterName = randomString(10);
        List<DcTbl> dcTbls = dcService.findAllDcs();

        ClusterModel clusterModel = new ClusterModel();

        clusterModel.setClusterTbl(new ClusterTbl()
                .setClusterName(clusterName)
                .setClusterType(ClusterType.BI_DIRECTION.toString())
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
        );

        List<DcClusterModel> dcClusters = new LinkedList<>();
        dcTbls.forEach(dcTbl -> {
            DcModel dcModel = new DcModel();
            dcModel.setDc_name(dcTbl.getDcName());
            dcClusters.add(new DcClusterModel().setDc(dcModel).setDcCluster(new DcClusterTbl()));
        });
        clusterModel.setDcClusters(dcClusters);
        clusterService.createCluster(clusterModel);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName)));

        //test clusterDesignated route
        String clusterName2 = randomString(10);
        clusterModel.setClusterTbl(new ClusterTbl()
                .setClusterName(clusterName2)
                .setClusterType(ClusterType.BI_DIRECTION.toString())
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
                .setClusterDesignatedRouteIds("1,2")
        );

        List<DcClusterModel> dcClusters1 = new LinkedList<>();
        dcTbls.forEach(dcTbl -> {
            DcModel dcModel = new DcModel();
            dcModel.setDc_name(dcTbl.getDcName());
            dcClusters1.add(new DcClusterModel().setDc(dcModel).setDcCluster(new DcClusterTbl()));
        });
        clusterModel.setDcClusters(dcClusters1);
        clusterService.createCluster(clusterModel);
        clusterTbl = clusterService.find(clusterName2);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName2)));
    }

    @Test
    public void testUpdateActiveDcId(){

        ClusterTbl clusterTbl = clusterService.find(clusterName);

        long oldActiveDcId = clusterTbl.getActivedcId();
        long newActiveDcId = clusterTbl.getActivedcId() + 1;
        clusterService.updateActivedcId(clusterTbl.getId(), newActiveDcId);

        ClusterTbl newCluster = clusterService.find(clusterName);

        Assert.assertEquals(newActiveDcId, newCluster.getActivedcId());
        newCluster.setActivedcId(oldActiveDcId);
        Assert.assertEquals(clusterTbl.toString(), newCluster.toString());
    }

    @Test
    public void testUpdateStatusById(){

        ClusterTbl clusterTbl = clusterService.find(clusterName);

        ClusterStatus oldStatus = ClusterStatus.valueOf(clusterTbl.getStatus());
        Assert.assertEquals(ClusterStatus.Normal, oldStatus);

        ClusterStatus newStatus = ClusterStatus.different(oldStatus);
        clusterService.updateStatusById(clusterTbl.getId(), newStatus, 100L);

        ClusterTbl newCluster = clusterService.find(clusterName);

        Assert.assertEquals(newStatus.toString(), newCluster.getStatus());
        Assert.assertEquals(100L, newCluster.getMigrationEventId());

        newCluster.setStatus(oldStatus.toString());
        newCluster.setMigrationEventId(0);
        Assert.assertEquals(clusterTbl.toString(), newCluster.toString());

    }

    @Test
    public void testUpdateClusterWithIncorrectInput() {
        long EXPECTED_ORG_ID = 0L;
        long SET_ORG_ID = 6L;
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        clusterTbl.setClusterOrgId(SET_ORG_ID);
        ClusterModel clusterModel = new ClusterModel().setClusterTbl(clusterTbl);
        clusterService.updateCluster(clusterName, clusterModel);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(EXPECTED_ORG_ID, clusterTbl.getClusterOrgId());
    }

    @Test
    public void testUpdateClusterPositive() {
        long EXPECTED_ORG_ID = 6L;
        organizationService.updateOrganizations();
        OrganizationTbl organizationTbl = organizationService.getOrganizationTblByCMSOrganiztionId(EXPECTED_ORG_ID);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        clusterTbl.setClusterOrgName(organizationTbl.getOrgName());
        ClusterModel clusterModel = new ClusterModel().setClusterTbl(clusterTbl);
        clusterService.updateCluster(clusterName, clusterModel);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals((long)organizationTbl.getId(), clusterTbl.getClusterOrgId());
    }

    @Test
    public void testCheckEmails() {
        ClusterServiceImpl service = new ClusterServiceImpl();
        String emails = "test@ctrip.com";
        Assert.assertTrue(service.checkEmails(emails));
        emails = "test@ctrip.com, test2@ctrip.com";
        Assert.assertTrue(service.checkEmails(emails));
        emails = "test@ctrip.com; test2@ctrip.com";
        Assert.assertTrue(service.checkEmails(emails));
        emails = "test@ctrip.com,test2@ctrip.com,test3@Ctrip.com";
        Assert.assertTrue(service.checkEmails(emails));
        emails = "tetsataemail@";
        Assert.assertFalse(service.checkEmails(emails));
    }

    @Test
    public void testBreakLoop() {
        int kCounter = 0, jCounter = 0, iCounter = 0;
        for(int i = 0; i < 10; i++) {
            iCounter ++;
            loop:
            for(int j = 0; j < 10; j++) {
                jCounter ++;
                for(int k = 0; k < 10; k++) {
                    kCounter ++;
                    if(k == 1) {
                        break loop;
                    }
                }
            }
        }
        Assert.assertEquals(10, iCounter);
        Assert.assertEquals(10, jCounter);
        Assert.assertEquals(20, kCounter);
    }

    @Test
    public void testFindAllClusterByKeeperContainer() {
        List<ClusterTbl> clusterTbls = clusterService.findAllClusterByKeeperContainer(4);
        Assert.assertTrue(clusterTbls.size() > 0);
        Assert.assertNotNull(clusterTbls.get(0).getOrganizationInfo());
    }

    @Test
    public void testGetClusterRelatedDcs() {
        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs("cluster101");
        Set<Long> dcSet = Sets.newHashSet();
        dcTbls.forEach(dcTbl -> dcSet.add(dcTbl.getId()));
        Assert.assertEquals(2, dcSet.size());
        Assert.assertTrue(dcSet.contains(1L));
        Assert.assertTrue(dcSet.contains(2L));
    }

    @Test
    public void testClusterExchangeName() {
        ClusterTbl former, latter;

        clusterService.exchangeName(101L, "cluster101", 102L, "cluster102");
        former = clusterService.find(101L);
        latter = clusterService.find(102L);
        Assert.assertEquals(former.getClusterName(), "cluster102");
        Assert.assertEquals(latter.getClusterName(), "cluster101");
        /* exchange name would fail if retry request */
        try {
            clusterService.exchangeName(101L, "cluster101", 102L, "cluster102");
            Assert.fail("exception expected");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not match"));
        }
        /* exchange name again to restore original status */
        clusterService.exchangeName(101L, "cluster102", 102L, "cluster101");
        former = clusterService.find(101L);
        latter = clusterService.find(102L);
        Assert.assertEquals(former.getClusterName(), "cluster101");
        Assert.assertEquals(latter.getClusterName(), "cluster102");
    }

    @Test
    public void testMigratingClusterNames() {
        Assert.assertTrue(clusterService.findMigratingClusterNames().isEmpty());
        ClusterTbl clusterTbl = clusterService.find("cluster101");

        MigrationRequest migrationRequest = new MigrationRequest("unit_test");
        MigrationRequest.ClusterInfo clusterInfo = new MigrationRequest.ClusterInfo(clusterTbl.getId(),
                clusterTbl.getClusterName(), 1, "jq", 2, "oy");
        migrationRequest.addClusterInfo(clusterInfo);
        migrationRequest.setTag("unit_test");
        migrationEventDao.createMigrationEvent(migrationRequest);

        Assert.assertEquals(Collections.singleton("cluster101"), clusterService.findMigratingClusterNames());
    }

    @Test
    public void testUpdateClusterDesignateRoutes() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1L);
        RouteInfoModel routeInfoModel2 = new RouteInfoModel().setId(2L);
        RouteInfoModel routeInfoModel3 = new RouteInfoModel().setId(3L);

        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        //test addClusterDesignateRoute
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel1));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test add other dc
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel3, routeInfoModel1));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test add and delete
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel2));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet( "2", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test add
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel1, routeInfoModel2));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1", "2", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test repeat add
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel1, routeInfoModel2, routeInfoModel1));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1", "2", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test empty list
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Collections.emptyList());
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));
    }

    @Test
    public void testUpdateClusterDesignateRoutesWithOneWayNotify() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1L);

        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        //test addClusterDesignateRoute
        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel1));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));
    }

    @Test
    public void testUpdateClusterDesignateRoutesWithBiClusterNotify() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1L);

        String biClusterName = "bi-cluster1";
        ClusterTbl clusterTbl = clusterService.find(biClusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        //test addClusterDesignateRoute
        clusterService.updateClusterDesignateRoutes(biClusterName, dcNames[0], Lists.newArrayList(routeInfoModel1));
        clusterTbl = clusterService.find(biClusterName);
        Assert.assertEquals(Sets.newHashSet("1"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

    }

    @Test
    public void testFindDesignateRoutesByDcNameAndClusterName() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1L);
        RouteInfoModel routeInfoModel3 = new RouteInfoModel().setId(3L);

        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());
        List<RouteInfoModel> designatedRoutes = clusterService.findClusterDesignateRoutesBySrcDcNameAndClusterName(dcNames[0], clusterName);
        Assert.assertEquals(0, designatedRoutes.size());

        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel1, routeInfoModel3));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));
        designatedRoutes = clusterService.findClusterDesignateRoutesBySrcDcNameAndClusterName(dcNames[0], clusterName);
        Assert.assertEquals(1, designatedRoutes.size());
        Assert.assertEquals(1L, designatedRoutes.get(0).getId());

        designatedRoutes = clusterService.findClusterDesignateRoutesBySrcDcNameAndClusterName(dcNames[1], clusterName);
        Assert.assertEquals(1, designatedRoutes.size());
        Assert.assertEquals(3L, designatedRoutes.get(0).getId());
    }

    @Test
    public void testFindUsedRoutesByDcNameAndClusterName() {
        String biClusterName = "bi-cluster1";

        String tunnelId1 =  "127.0.0.1:1880-R(127.0.0.1:1880)-L(1.1.1.1:80)->R(1.1.1.2:443)-TCP://127.0.0.3:6380";
        ProxyModel proxyModel1 = new ProxyModel().setActive(true).setDcName("jq").setId(1).setUri("PROXYTCP://1.1.1.1:80");
        List<TunnelInfo> tunnelInfo1 = Lists.newArrayList(new DefaultTunnelInfo(proxyModel1, tunnelId1));
        ProxyChain proxyChain1 = new DefaultProxyChain("jq", biClusterName, shardNames[0],"oy", tunnelInfo1);

        List<RouteInfoModel> allDcRoutes = routeService.getAllActiveRouteInfoModelsByTagAndSrcDcName(Route.TAG_META, "jq");

        RouteInfoModel route = clusterService.getRouteInfoModelFromProxyChainModel(allDcRoutes, new ProxyChainModel(proxyChain1, proxyChain1.getPeerDcId(), "jq"));
        Assert.assertEquals(1L, route.getId());

        String tunnelId2 = "127.0.0.1:1880-R(127.0.0.1:1880)-L(1.1.1.3:80)->R(1.1.1.4:443)-TCP://127.0.0.3:6380";
        String tunnelId3 = "127.0.0.1:1880-R(1.1.0.3:1880)-L(1.1.0.4:80)->R(1.1.1.5:443)-TCP://127.0.0.3:6380";
        String tunnelId4 = "127.0.0.1:1880-R(1.1.0.4:333)-L(1.1.0.5:80)->R(127.0.0.3:443)-TCP://127.0.0.3:6380";
        ProxyModel proxyModel2 = new ProxyModel().setActive(true).setDcName("jq").setId(3);
        ProxyModel proxyModel3 = new ProxyModel().setActive(true).setDcName("fra").setId(4);
        ProxyModel proxyModel4 = new ProxyModel().setActive(true).setDcName("oy").setId(20);
        List<TunnelInfo> tunnelInfos2 = Lists.newArrayList(new DefaultTunnelInfo(proxyModel2, tunnelId2),
                new DefaultTunnelInfo(proxyModel3, tunnelId3), new DefaultTunnelInfo(proxyModel4, tunnelId4));
        ProxyChain proxyChain2 = new DefaultProxyChain("jq", biClusterName, shardNames[0],"oy", tunnelInfos2);

        route = clusterService.getRouteInfoModelFromProxyChainModel(allDcRoutes, new ProxyChainModel(proxyChain2, proxyChain1.getPeerDcId(), "jq"));
        Assert.assertEquals(2L, route.getId());

    }

    @Test
    public void testParseDstDcs() {
        //test one_way
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        List<String> dstDcs = clusterService.parseDstDcs(clusterTbl);
        Assert.assertEquals(1, dstDcs.size());
        Assert.assertEquals("jq", dstDcs.get(0));

        String biClusterName = "bi-cluster1";
        clusterTbl = clusterService.find(biClusterName);
        dstDcs = clusterService.parseDstDcs(clusterTbl);
        Assert.assertEquals(2, dstDcs.size());
        Assert.assertEquals(Lists.newArrayList("jq", "oy"), dstDcs);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test2.sql");
    }

}
