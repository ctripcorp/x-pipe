package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
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

        clusterModel.setDcs(dcTbls);
        clusterService.createCluster(clusterModel);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName)));

        //test clusterDesignterRoute
        String clusterName2 = randomString(10);
        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(1)
                .setClusterName(clusterName2)
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
                .setClusterDesignatedRouteIds("1,2")
        );
        clusterModel.setDcs(dcTbls);
        clusterService.createCluster(clusterModel);
        clusterTbl = clusterService.find(clusterName2);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName2)));

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

        clusterModel.setDcs(dcTbls);
        clusterService.createCluster(clusterModel);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName)));

        //test clusterDesinated route
        String clusterName2 = randomString(10);
        clusterModel.setClusterTbl(new ClusterTbl()
                .setClusterName(clusterName2)
                .setClusterType(ClusterType.BI_DIRECTION.toString())
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
                .setClusterDesignatedRouteIds("1,2")
        );

        clusterModel.setDcs(dcTbls);
        clusterService.createCluster(clusterModel);
        clusterTbl = clusterService.find(clusterName2);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

        Assert.assertFalse(dcTbls.isEmpty());
        dcTbls.forEach(dcTbl -> Assert.assertNotNull(dcClusterService.find(dcTbl.getDcName(), clusterName2)));
    }

    @Test
    public void testUpdateActivedcId(){

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
        clusterService.updateCluster(clusterName, clusterTbl);
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
        clusterService.updateCluster(clusterName, clusterTbl);
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
        dcTbls.forEach(dcTbl -> {dcSet.add(dcTbl.getId());});
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
        RouteInfoModel routeInfoModel2 = new RouteInfoModel().setId(2L);
        RouteInfoModel routeInfoModel3 = new RouteInfoModel().setId(3L);

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
        List<RouteInfoModel> designatedRoutes = clusterService.findClusterDesignateRoutesByDcNameAndClusterName(dcNames[0], clusterName);
        Assert.assertEquals(0, designatedRoutes.size());

        clusterService.updateClusterDesignateRoutes(clusterName, dcNames[0], Lists.newArrayList(routeInfoModel1, routeInfoModel3));
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));
        designatedRoutes = clusterService.findClusterDesignateRoutesByDcNameAndClusterName(dcNames[0], clusterName);
        Assert.assertEquals(1, designatedRoutes.size());
        Assert.assertEquals(1L, designatedRoutes.get(0).getId());

        designatedRoutes = clusterService.findClusterDesignateRoutesByDcNameAndClusterName(dcNames[1], clusterName);
        Assert.assertEquals(1, designatedRoutes.size());
        Assert.assertEquals(3L, designatedRoutes.get(0).getId());
    }

    @Test
    public void testFindDefaultRoutesByDcNameAndClusterNameWithOneWay() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1L);
        RouteInfoModel routeInfoModel3 = new RouteInfoModel().setId(3L);
        RouteInfoModel routeInfoModel4 = new RouteInfoModel().setId(4L);
        RouteInfoModel routeInfoModel5 = new RouteInfoModel().setId(5L);
        RouteInfoModel routeInfoModel6 = new RouteInfoModel().setId(6L);

        String clusterName101 = "cluster101";
        ClusterTbl clusterTbl = clusterService.find("cluster101");
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        //test same org_id
        List<RouteInfoModel> defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], clusterName101);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(3L, defaultRoutes.get(0).getId());

        //test Hash strategy
        routeService.updateRoute(new RouteModel().setId(4L).setActive(true).setTag(Route.TAG_META).setPublic(true).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(new ClusterServiceImpl.Crc32HashChooseRouteStrategy(clusterName101).chooseRouteInfoModel(Lists.newArrayList(routeInfoModel3, routeInfoModel4)).getId(),
                defaultRoutes.get(0).getId());

        routeService.updateRoute(new RouteModel().setId(3L).setActive(true).setTag(Route.TAG_META).setPublic(false).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));

        routeService.updateRoute(new RouteModel().setId(4L).setActive(true).setTag(Route.TAG_META).setPublic(false).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));

        //test default org_id
        defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], clusterName101);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(6L, defaultRoutes.get(0).getId());

        //test hash strategy
        routeService.updateRoute(new RouteModel().setId(5L).setActive(true).setTag(Route.TAG_META).setPublic(true).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));
        defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], clusterName101);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(new ClusterServiceImpl.Crc32HashChooseRouteStrategy(clusterName101).chooseRouteInfoModel(Lists.newArrayList(routeInfoModel5, routeInfoModel6)).getId(),
                defaultRoutes.get(0).getId());


        //test designated route has no effect
        clusterService.updateClusterDesignateRoutes(clusterName101, dcNames[0], Lists.newArrayList(routeInfoModel1, routeInfoModel3));
        defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], clusterName101);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(new ClusterServiceImpl.Crc32HashChooseRouteStrategy(clusterName101).chooseRouteInfoModel(Lists.newArrayList(routeInfoModel5, routeInfoModel6)).getId(),
                defaultRoutes.get(0).getId());
    }

    @Test
    public void testFindDefaultRoutesByDcNameAndClusterNameWithBiDirection() {
        RouteInfoModel routeInfoModel1 = new RouteInfoModel().setId(1L);
        RouteInfoModel routeInfoModel3 = new RouteInfoModel().setId(3L);
        RouteInfoModel routeInfoModel4 = new RouteInfoModel().setId(4L);
        RouteInfoModel routeInfoModel5 = new RouteInfoModel().setId(5L);
        RouteInfoModel routeInfoModel6 = new RouteInfoModel().setId(6L);

        String biClusterName = "bi-cluster1";
        ClusterTbl clusterTbl = clusterService.find(biClusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        //test same org_id
        List<RouteInfoModel> defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], biClusterName);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(3L, defaultRoutes.get(0).getId());

        //test Hash strategy
        routeService.updateRoute(new RouteModel().setId(4L).setActive(true).setTag(Route.TAG_META).setPublic(true).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(new ClusterServiceImpl.Crc32HashChooseRouteStrategy(biClusterName).chooseRouteInfoModel(Lists.newArrayList(routeInfoModel4, routeInfoModel3)).getId(),
                defaultRoutes.get(0).getId());

        routeService.updateRoute(new RouteModel().setId(3L).setActive(true).setTag(Route.TAG_META).setPublic(false).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));

        routeService.updateRoute(new RouteModel().setId(4L).setActive(true).setTag(Route.TAG_META).setPublic(false).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));

        //test default org_id
        defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], biClusterName);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(6L, defaultRoutes.get(0).getId());

        //test hash strategy
        routeService.updateRoute(new RouteModel().setId(5L).setActive(true).setTag(Route.TAG_META).setPublic(true).setDstDcName("jq").setOrgId(1L)
                .setSrcProxyIds("5").setDstProxyIds("6").setSrcDcName("oy"));
        defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], biClusterName);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(new ClusterServiceImpl.Crc32HashChooseRouteStrategy(biClusterName).chooseRouteInfoModel(Lists.newArrayList(routeInfoModel6, routeInfoModel5)).getId(),
                defaultRoutes.get(0).getId());


        //test designated route has no effect
        clusterService.updateClusterDesignateRoutes(biClusterName, dcNames[0], Lists.newArrayList(routeInfoModel1, routeInfoModel3));
        defaultRoutes = clusterService.findClusterDefaultRoutesByDcNameAndClusterName(dcNames[1], biClusterName);
        Assert.assertEquals(1, defaultRoutes.size());
        Assert.assertEquals(new ClusterServiceImpl.Crc32HashChooseRouteStrategy(biClusterName).chooseRouteInfoModel(Lists.newArrayList(routeInfoModel5, routeInfoModel5)).getId(),
                defaultRoutes.get(0).getId());

    }

    @Test
    public void testFindUsedRoutesByDcNameAndClusterName() {
        String biClusterName = "bi-cluster1";

        String tunnelId1 = "127.0.0.1:1880-R(127.0.0.1:1880)-L(172.19.0.20:80)->R(172.19.0.21:%d)-TCP://127.0.0.3:6380";
        ProxyModel proxyModel1 = new ProxyModel().setActive(true).setDcName("jq").setId(1).setUri("PROXYTCP://172.19.0.20:8080");
        List<TunnelInfo> tunnelInfos1 = Lists.newArrayList(new DefaultTunnelInfo(proxyModel1, tunnelId1));
        ProxyChain proxyChain1 = new DefaultProxyChain("jq", biClusterName, shardNames[0],"oy", tunnelInfos1);

        List<RouteInfoModel> allDcRoutes = routeService.getAllActiveRouteInfoModelsByTagAndSrcDcName(Route.TAG_META, "jq");

        RouteInfoModel route = clusterService.getRouteInfoModelFromProxyChainModel(allDcRoutes, new ProxyChainModel(proxyChain1, proxyChain1.getPeerDcId(), "jq"));
        Assert.assertEquals(1L, route.getId());

        String tunnelId2 = "127.0.0.1:1880-R(127.0.0.1:1880)-L(172.19.0.22:80)->R(527.77.103.203:443)-TCP://127.0.0.3:6380";
        String tunnelId3 = "127.0.0.1:1880-R(103.158.15.65:1880)-L(172.19.0.90:80)->R(172.19.0.23:443)-TCP://127.0.0.3:6380";
        String tunnelId4 = "127.0.0.1:1880-R(127.0.0.1:1880)-L(172.19.0.90:80)->R(172.19.0.23:443)-TCP://127.0.0.3:6380";
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
    public void testFindUnmatchedClusterRoutes() {
        clusterService.findUnmatchedClusterRoutes();
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test2.sql");
    }

}
