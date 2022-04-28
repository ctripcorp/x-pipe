package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterExchangeNameInfo;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;


public class MetaUpdateTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterDao clusterDao;

    @Autowired
    private MetaUpdate clusterController;

    @Test
    public void updateCluster() throws Exception {
        String CLUSTER_NAME = "cluster-name";
        int SUCCESS_STATE = RetMessage.SUCCESS_STATE;
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
        Assert.assertEquals(SUCCESS_STATE, retMessage.getState());

        ClusterTbl cluster = clusterDao.findClusterAndOrgByName(CLUSTER_NAME);
        Assert.assertEquals(ORG_ID, cluster.getOrganizationInfo().getOrgId());
    }

    @Test
    public void testUpdateClusterWithNoClusterFound() throws Exception {
        String CLUSTER_NAME = "cluster-not-exist";
        String EXPECTED_MESSAGE = String.format("cluster not found: %s", CLUSTER_NAME);
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterName(CLUSTER_NAME);

        RetMessage retMessage = clusterController.updateCluster(clusterCreateInfo);
        logger.info("{}", retMessage.getMessage());
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());
        Assert.assertEquals(EXPECTED_MESSAGE, retMessage.getMessage());
    }

    @Test
    public void testUpdateCLusterWithNoNeedUpdate() throws Exception {
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
    public void testUpdateCLusterWithNoOrgIDFound() throws Exception {
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
        retMessage = clusterController.clusterExchangeName(exinfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());

        /* suc on first exchange attempt */
        exinfo.setFormerClusterId(FORMER_ID);
        exinfo.setFormerClusterName(FORMER_NAME);
        exinfo.setLatterClusterId(LATTER_ID);
        exinfo.setLatterClusterName(LATTER_NAME);
        retMessage = clusterController.clusterExchangeName(exinfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());

        /* fail on retry exchange attempt */
        retMessage = clusterController.clusterExchangeName(exinfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, retMessage.getState());

        /* suc on exchage back */
        exinfo.setFormerClusterName(LATTER_NAME);
        exinfo.setLatterClusterName(FORMER_NAME);
        retMessage = clusterController.clusterExchangeName(exinfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, retMessage.getState());
    }
}