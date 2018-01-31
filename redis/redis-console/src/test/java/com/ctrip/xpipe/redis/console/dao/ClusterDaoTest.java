package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.util.List;

/**
 * @author chen.zhu
 *
 * Sep 04, 2017
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

    @Test
    public void testDeleteCluster() throws Exception {
        clusterDao.createCluster(clusterTbl);
        clusterDao.deleteCluster(clusterTbl);
    }

    @Test
    public void testFindClustersWithName() throws Exception {
        List<ClusterTbl> clusters = clusterDao.findClustersWithName(Lists.newArrayList());
        Assert.assertTrue(clusters.isEmpty());

        clusters = clusterDao.findClustersWithName(Lists.newArrayList("cluster2"));
        logger.info("clusters: {}", clusters);
        Assert.assertEquals(1, clusters.size());

        clusters = clusterDao.findClustersWithName(Lists.newArrayList("cluster2", "cluster-not-exist"));
        logger.info("clusters: {}", clusters);
        Assert.assertEquals(1, clusters.size());

        logger.info("{}", clusterDao.findAllClusterWithOrgInfo());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return "insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,is_xpipe_interested, cluster_org_id) values (2,'cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Normal',1, 2);\n"
                + "\n"
                + "insert into organization_tbl(org_id, org_name) values (1, 'org-1'), (2, 'org-2'), (3, 'org-3'), (4, 'org-4'), (5, 'org-5'), (6, 'org-6')";
    }
}
