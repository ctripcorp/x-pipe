package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 06, 2017
 */
public class KeepercontainerServiceImplTest extends AbstractServiceImplTest{


    @Autowired
    private KeepercontainerServiceImpl keepercontainerService;

    @Autowired
    private ClusterDao clusterDao;

    @Before
    public void beforeAbstractServiceImpl(){
    }

    @Test
    public void testKeeperCount(){

        List<KeepercontainerTbl> keeperCount = keepercontainerService.findKeeperCount(dcNames[0]);
        keeperCount.forEach((keepercontainerTbl) -> {
            logger.info("{}", keepercontainerTbl);
        });

    }

    @Test
    public void testFindKeeperCountByClusterWithBUSpecified() {
        String clusterName = "cluster2";
        long orgId = 2L;
        List<KeepercontainerTbl> keeperCount = keepercontainerService.findBestKeeperContainersByDcCluster(dcNames[0], clusterName);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == orgId));
    }

    @Test
    public void testFindKeeperCountByClusterWithBUSpecifiedAndContainsKeepers() {
        String clusterName = "cluster5";
        long orgId = 5L;
        List<KeepercontainerTbl> keeperCount = keepercontainerService.findBestKeeperContainersByDcCluster(dcNames[0], clusterName);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == orgId));
    }

    @Test
    public void testFindKeeperCountByClusterWithNoneBUSpecified() {
        String clusterName = "cluster1";
        long orgId = XPipeConsoleConstant.DEFAULT_ORG_ID;
        List<KeepercontainerTbl> keeperCount = keepercontainerService.findBestKeeperContainersByDcCluster(dcNames[0], clusterName);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == orgId));
    }

    @Test
    public void testFindKeeperCountByClusterWithNoKCForBU() {
        String clusterName = "cluster3";
        long orgId = XPipeConsoleConstant.DEFAULT_ORG_ID;
        List<KeepercontainerTbl> keeperCount = keepercontainerService.findBestKeeperContainersByDcCluster(dcNames[0], clusterName);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == orgId));
    }

    @Test
    public void testFindKeeperCountByClusterWithAllKeeperDeleted() {
        String clusterName = "cluster4";
        ClusterTbl clusterTbl = clusterDao.findClusterAndOrgByName(clusterName);
        List<KeepercontainerTbl> keeperCount = keepercontainerService.findBestKeeperContainersByDcCluster(dcNames[0], clusterName);
        keeperCount.forEach(kc -> logger.info("{}", kc));
        Assert.assertTrue(keeperCount.stream().allMatch(kc->kc.getKeepercontainerOrgId() == clusterTbl.getClusterOrgId()));
    }

    @Test
    public  void testFindAllActiveByDcName(){

        String dcName = dcNames[0];
        List<KeepercontainerTbl> allByDcName = keepercontainerService.findAllByDcName(dcName);

        int size = allByDcName.size();
        Assert.assertTrue(size > 0);
        for(KeepercontainerTbl keepercontainerTbl : allByDcName){
            Assert.assertTrue(keepercontainerTbl.isKeepercontainerActive());

            keepercontainerTbl.setKeepercontainerActive(false);
            keepercontainerService.update(keepercontainerTbl);
        }

        List<KeepercontainerTbl> allActiveByDcName = keepercontainerService.findAllActiveByDcName(dcName);
        Assert.assertEquals(0, allActiveByDcName.size());

        allByDcName = keepercontainerService.findAllByDcName(dcName);
        Assert.assertEquals(size, allByDcName.size());

    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/keeper-container-service-impl-test.sql");
    }
}
