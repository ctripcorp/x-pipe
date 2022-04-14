package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
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
    private ClusterService clusterService;

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
    public void testClusterDesignateRouteChange() {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        //test addClusterDesignateRoute
        clusterService.addClusterDesignateRoute(clusterName, 1L);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        clusterService.addClusterDesignateRoute(clusterName, 2L);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("1", "2"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test update
        clusterService.updateClusterDesignateRoute(clusterName, 1L, 3L);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("2", "3"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        clusterService.updateClusterDesignateRoute(clusterName, 3L, 4L);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("2", "4"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test deleteClusterDesignateRoute
        clusterService.deleteClusterDesignateRoute(clusterName, 2L);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet("4"), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));

        //test deleteClusterDesignateRoute
        clusterService.deleteClusterDesignateRoute(clusterName, 4L);
        clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals(Sets.newHashSet(""), Sets.newHashSet(clusterTbl.getClusterDesignatedRouteIds().split(",")));
    }

    @Test(expected = BadRequestException.class)
    public void testDeleteClusterDesignateRouteFail() {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        try {
            clusterService.deleteClusterDesignateRoute(clusterName, 1L);
        } catch (Exception e) {
            Assert.assertEquals("this cluster has no designated routes!", e.getMessage());
            throw e;
        }
    }

    @Test(expected = BadRequestException.class)
    public void testUpdateClusterDesignateRouteFail() {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertEquals("", clusterTbl.getClusterDesignatedRouteIds());

        try {
            clusterService.updateClusterDesignateRoute(clusterName, 1L, 2L);
        } catch (Exception e) {
            Assert.assertEquals("this cluster has no designated routes!", e.getMessage());
            throw e;
        }
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test2.sql");
    }

}
