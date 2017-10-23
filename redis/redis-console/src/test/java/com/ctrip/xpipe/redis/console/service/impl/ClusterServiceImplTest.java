package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

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

        Assert.assertEquals(newStatus.toString(), newCluster.getStatus().toString());

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
        emails = "test@gmail.com, test2@ctrip.com";
        Assert.assertFalse(service.checkEmails(emails));
        emails = "tetsataemail@";
        Assert.assertFalse(service.checkEmails(emails));
    }
}
