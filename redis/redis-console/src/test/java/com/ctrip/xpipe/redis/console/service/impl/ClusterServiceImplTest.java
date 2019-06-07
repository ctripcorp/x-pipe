package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.google.common.collect.Maps;
import org.junit.*;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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



    @Test
    public void testCreateCluster(){

        String clusterName = randomString(10);
        ClusterModel clusterModel = new ClusterModel();

        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(1)
                .setClusterName(clusterName)
                .setClusterAdminEmails("test@ctrip.com")
                .setClusterDescription(randomString(20))
        );

        clusterService.createCluster(clusterModel);
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        Assert.assertTrue(clusterTbl.isIsXpipeInterested());

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
        ClusterStatus newStatus = ClusterStatus.different(oldStatus);

        clusterService.updateStatusById(clusterTbl.getId(), newStatus);

        ClusterTbl newCluster = clusterService.find(clusterName);

        Assert.assertEquals(newStatus.toString(), newCluster.getStatus());

        newCluster.setStatus(oldStatus.toString());
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
    public void testReBalanceSentinels() {
        List<String> clusters = clusterService.reBalanceSentinels(1);
        Assert.assertEquals(1, clusters.size());
        logger.info("Changed clusters: {}", clusters);
    }

    @Test
    public void testReBalanceSentinels2() {
        List<String> clusters = clusterService.reBalanceSentinels(10);
        Assert.assertEquals(clusterService.findAllClusterNames().size(), clusters.size());
        logger.info("Changed clusters: {}", clusters);
    }

    @Test
    public void testRandomlyChoseSentinels() {
        List<SetinelTbl> setinelTbls = Arrays.asList(new SetinelTbl().setSetinelId(999L),
                new SetinelTbl().setSetinelId(1000L), new SetinelTbl().setSetinelId(9999L));
        ClusterServiceImpl clusterServiceImpl = new ClusterServiceImpl();
        long id = clusterServiceImpl.randomlyChoseSentinels(setinelTbls);
        logger.info("id: {}", id);
    }

    @Test
    public void testBalanceCluster() throws Exception {
        List<SetinelTbl> setinelTbls1 = Arrays.asList(new SetinelTbl().setSetinelId(999L),
                new SetinelTbl().setSetinelId(1000L), new SetinelTbl().setSetinelId(9999L));
        List<SetinelTbl> setinelTbls2 = Arrays.asList(new SetinelTbl().setSetinelId(1999L),
                new SetinelTbl().setSetinelId(11000L), new SetinelTbl().setSetinelId(19999L));
        Map<String, List<SetinelTbl>> map = Maps.newHashMap();
        map.put(dcNames[0], setinelTbls1);
        map.put(dcNames[1], setinelTbls2);
        ClusterTbl clusterTbl = clusterService.find(clusterName);

        List<DcTbl> dcTbls = dcService.findAllDcs();
        logger.info("dcTbls: {}", dcTbls);
        long []dcIds = new long[] {dcTbls.get(0).getId(), dcTbls.get(1).getId()};

        Map<Long, SetinelTbl> dcToSentinel = Maps.newHashMap();
        dcToSentinel.put(dcIds[0], setinelTbls1.get(0));
        dcToSentinel.put(dcIds[1], setinelTbls2.get(0));
        ShardTbl shard1 = new ShardTbl().setShardName("cluster1shard1test").setSetinelMonitorName("cluster1shard1monitor")
                .setClusterId(clusterTbl.getId()).setDeleted(false).setClusterInfo(clusterTbl);
        ShardTbl shard2 = new ShardTbl().setShardName("cluster1shard2test").setSetinelMonitorName("cluster1shard2monitor")
                .setClusterId(clusterTbl.getId()).setDeleted(false).setClusterInfo(clusterTbl);
        shardService.createShard(clusterName, shard1, dcToSentinel);
        dcToSentinel.put(dcIds[0], setinelTbls1.get(1));
        dcToSentinel.put(dcIds[1], setinelTbls2.get(1));
        shardService.createShard(clusterName, shard2, dcToSentinel);

        int checkTimes = 10;
        while(checkTimes -- > 0) {
            clusterService.reBalanceSentinels(10);
            DcClusterShardTbl dcClusterShardTbl1 = dcClusterShardService.find(dcNames[0], clusterName, shard1.getShardName());
            DcClusterShardTbl dcClusterShardTbl2 = dcClusterShardService.find(dcNames[0], clusterName, shard2.getShardName());
            Assert.assertEquals(dcClusterShardTbl1.getSetinelId(), dcClusterShardTbl2.getSetinelId());
        }
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

}
