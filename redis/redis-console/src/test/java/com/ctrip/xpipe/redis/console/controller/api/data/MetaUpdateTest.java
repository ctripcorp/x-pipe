package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
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
                                            .setClusterDescription("")
                                            .setActivedcId(1)
                                            .setIsXpipeInterested(true)
                                            .setStatus("normal")
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
                .setClusterDescription("")
                .setActivedcId(1)
                .setIsXpipeInterested(true)
                .setStatus("normal")
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
                .setClusterDescription("")
                .setActivedcId(1)
                .setIsXpipeInterested(true)
                .setStatus("normal")
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

}