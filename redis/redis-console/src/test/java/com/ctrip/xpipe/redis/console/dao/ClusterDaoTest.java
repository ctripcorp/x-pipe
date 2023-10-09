package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

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
            .setClusterName("ut-cluster")
            .setClusterType(ClusterType.ONE_WAY.toString())
            .setActivedcId(1)
            .setClusterDescription("ut-cluster")
            .setClusterOrgId(0L)
            .setCount(12)
            .setIsXpipeInterested(true)
            .setClusterLastModifiedTime("test-last-modified")
            .setStatus("normal")
            .setClusterDesignatedRouteIds("");
    }


    @Test
    public void testCreateCluster() {
        ClusterTbl newCluster = clusterDao.createCluster(clusterTbl);
        Assert.assertEquals(clusterTbl.getId(), newCluster.getId());
    }

    @Test
    public void testFindClusterAndOrgByName() {
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
        ClusterTbl cluster = clusterDao.findClusterByClusterName(clusterTbl.getClusterName());
        Assert.assertNotNull(cluster);
        clusterDao.deleteCluster(clusterTbl);
        cluster = clusterDao.findClusterByClusterName(clusterTbl.getClusterName());
        Assert.assertNull(cluster);
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

    @Test
    public void testFindMigratingClustersWithEvents() {
        List<ClusterTbl> clusters = clusterDao.findMigratingClustersWithEvents();
        Assert.assertEquals(2, clusters.size());
        for (ClusterTbl cluster : clusters) {
            switch (cluster.getClusterName()) {
                case "cluster11":
                    Assert.assertEquals(10001, cluster.getMigrationEvent().getId());
                    continue;
                case "cluster10":
                    Assert.assertEquals(0, cluster.getMigrationEvent().getId());
                    continue;
            }
        }
    }

    @Override
    protected String prepareDatas() throws IOException {
        return "insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status, is_xpipe_interested, cluster_org_id) values (2,'cluster2',1,'Cluster:cluster2 , ActiveDC : A','0000000000000000','Normal',1, 2);\n"
            + "insert into organization_tbl(org_id, org_name) values (1, 'org-1'), (2, 'org-2'), (3, 'org-3'), (4, 'org-4'), (5, 'org-5'), (6, 'org-6');\n"
            + "insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status, is_xpipe_interested, cluster_org_id, migration_event_id) values (10,'cluster10',1,'Cluster:cluster10 , ActiveDC : A','0000000000000000','Migrating', 1, 2, 10000), (11,'cluster11',1,'Cluster:cluster11 , ActiveDC : A','0000000000000000','Migrating', 1, 2, 10001);\n"
            + "insert into MIGRATION_EVENT_TBL (id, start_time, operator, event_tag, DataChange_LastTime, deleted, break) values (10001,'2021-04-25 14:19:46.000000', 'wucc', '20210425141946587-wucc', '2021-04-25 14:20:10.000000', 0, 0)";
    }
}
