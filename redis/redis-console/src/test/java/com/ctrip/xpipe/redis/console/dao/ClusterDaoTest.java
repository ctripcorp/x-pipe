package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhuchen on 2017/8/26 0026.
 */
public class ClusterDaoTest  extends AbstractConsoleIntegrationTest {
    @Autowired
    ClusterDao clusterDao;

    ClusterTbl clusterTbl;

    @Autowired
    OrganizationService organizationService;

    @Before
    public void beforeClusterDaoTest() {
        clusterTbl = new ClusterTbl()
                .setClusterDescription("ut-cluster")
                .setActivedcId(1)
                .setClusterName("ut-cluster")
                .setCount(12)
                .setIsXpipeInterested(true)
                .setClusterLastModifiedTime("test-last-modified")
                .setStatus("normal");
    }


    @Test
    public void testCreateCluster() throws DalException {
        ClusterTbl newCluster = clusterDao.createCluster(clusterTbl);
        Assert.assertEquals(clusterTbl.getId(), newCluster.getId());
    }

    @Test
    public void testFindClusterAndOrgByName() throws DalException {
        String clusterName = "cluster2";
        ClusterTbl clusterTbl1 = clusterDao.findClusterAndOrgByName(clusterName);
        List<OrganizationTbl> orgs = organizationService.getAllOrganizations();
        orgs.forEach(org->logger.info("{}", org));
        logger.info("{}", clusterTbl1);
        Assert.assertEquals(2L, (long)clusterTbl1.getOrganizationInfo().getId());
        Assert.assertEquals("org-1", clusterTbl1.getClusterOrgName());
    }

    @Test
    public void testFindAllClustersWithOrgInfo() {
        List<ClusterTbl> clusterTblList = clusterDao.findAllClusterWithOrgInfo();
        clusterTblList.forEach(cluster->logger.info("{}", cluster));
    }

    @Test
    public void testFindAllClustersWithOrgInfoByActiveDc() {
        List<ClusterTbl> clusterTblList = clusterDao.findClustersWithOrgInfoByActiveDcId(1L);
        clusterTblList.forEach(cluster->logger.info("{}", cluster));
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-dao-test.sql");
    }
}
