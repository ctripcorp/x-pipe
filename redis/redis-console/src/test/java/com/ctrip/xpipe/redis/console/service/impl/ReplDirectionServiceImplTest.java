package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ReplDirectionTbl;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class ReplDirectionServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    ReplDirectionServiceImpl replDirectionService;

    @Autowired
    ClusterServiceImpl clusterService;

    @Test
    public void testFindReplDirectionInfoModelById(){
        ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(1);

        Assert.assertNotNull(replDirection);
        Assert.assertEquals(1, replDirection.getId());
        Assert.assertEquals(7,replDirection.getClusterId());
        Assert.assertEquals("jq",replDirection.getSrcDcName());
        Assert.assertEquals("jq",replDirection.getFromDcName());
        Assert.assertEquals("oy",replDirection.getToDcName());

        replDirection = replDirectionService.findReplDirectionInfoModelById(30);
        Assert.assertNull(replDirection);
    }

    @Test
    public void testFindAllReplDirectionByCluster() {
        List<ReplDirectionTbl> replDirectionTbls = replDirectionService.findAllReplDirectionTblsByCluster(18);
        Assert.assertEquals(0, replDirectionTbls.size());

        replDirectionTbls = replDirectionService.findAllReplDirectionTblsByCluster(7);
        Assert.assertEquals(2, replDirectionTbls.size());
    }

    @Test
    public void testReplDirectionInfoModelByClusterAndSrcDcAndToDc() {
        ReplDirectionInfoModel expect = new ReplDirectionInfoModel().setId(2L).setClusterName("hetero-cluster")
                                                .setSrcDcName("jq").setFromDcName("jq").setToDcName("fra");
        ReplDirectionInfoModel infoModel
                = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc("hetero-cluster", "jq", "fra");
        Assert.assertEquals(true, ObjectUtils.equals(expect, infoModel));

        infoModel = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc("hetero-cluster", "oy", "fra");
        Assert.assertNull(infoModel);

        try {
            infoModel = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc("none", "jq", "fra");
        } catch (Exception e) {
            Assert.assertEquals( String.format("cluster %s does not exist", "none"), e.getMessage());
        }

        try {
            infoModel = replDirectionService.findReplDirectionInfoModelByClusterAndSrcToDc("hetero-cluster", "none", "fra");
        } catch (Exception e) {
            Assert.assertEquals(String.format("src dc %s or to dc %s does not exist", "none", "fra"), e.getMessage());
        }
    }

    @Test
    public void testReplDirectionInfoModelsByClusterAndToDc(){

        List<ReplDirectionInfoModel> infoModels
                = replDirectionService.findReplDirectionInfoModelsByClusterAndToDc("hetero-cluster",  "fra");
        Assert.assertEquals(1, infoModels.size());

        infoModels = replDirectionService.findReplDirectionInfoModelsByClusterAndToDc("hetero-cluster2", "fra");
        Assert.assertEquals(2, infoModels.size());

        try {
            infoModels = replDirectionService.findReplDirectionInfoModelsByClusterAndToDc("none", "fra");
        } catch (Exception e) {
            Assert.assertEquals( String.format("cluster %s does not exist", "none"), e.getMessage());
        }

        try {
            infoModels = replDirectionService.findReplDirectionInfoModelsByClusterAndToDc("hetero-cluster", "none");
        } catch (Exception e) {
            Assert.assertEquals(String.format("dc %s does not exist", "none"), e.getMessage());
        }
    }

    @Test
    public void testFindAllReplDirectionInfoModelsByCluster() {
        String clusterName = "hetero-cluster";
        ReplDirectionInfoModel replDirectionInfoModel1 = new ReplDirectionInfoModel().setId(1)
                .setClusterName(clusterName).setSrcDcName("jq").setFromDcName("jq").setToDcName("oy");
        ReplDirectionInfoModel replDirectionInfoModel2 = new ReplDirectionInfoModel().setId(2)
                .setClusterName(clusterName).setSrcDcName("jq").setFromDcName("jq").setToDcName("fra");

        List<ReplDirectionInfoModel> replDirectionInfoModels =
                replDirectionService.findAllReplDirectionInfoModelsByCluster(clusterName);
        Assert.assertEquals(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2), replDirectionInfoModels);
    }

    @Test
    public void testFindAllReplDirectionInfoModels() {
        List<ReplDirectionInfoModel> allReplDirectionInfoModels = replDirectionService.findAllReplDirectionInfoModels();
        Assert.assertEquals(5, allReplDirectionInfoModels.size());
        Assert.assertEquals(2, allReplDirectionInfoModels.get(1).getSrcShardCount());
        Assert.assertEquals(1, allReplDirectionInfoModels.get(1).getToShardCount());
        Assert.assertEquals(8, allReplDirectionInfoModels.get(1).getKeeperCount());
        Assert.assertEquals(4, allReplDirectionInfoModels.get(1).getApplierCount());

    }

    @Test
    public void testFindAllReplDirectionJoinClusterTbl() {
        List<ReplDirectionTbl> allReplDirectionJoinClusterTbl = replDirectionService.findAllReplDirectionJoinClusterTbl();
        Assert.assertEquals(5, allReplDirectionJoinClusterTbl.size());
        allReplDirectionJoinClusterTbl.forEach(replDirectionTbl -> {
            Assert.assertNotEquals(0, replDirectionTbl.getSrcDcId());
            Assert.assertNotEquals(0, replDirectionTbl.getFromDcId());
            Assert.assertNotNull(replDirectionTbl.getClusterInfo());
        });
    }

    @Test
    public void testFindByClusterAndSrcToDc() {
        ReplDirectionTbl replication = replDirectionService.findByClusterAndSrcToDc("hetero-cluster", "jq", "fra");
        Assert.assertEquals(2, replication.getId());

        replication = replDirectionService.findByClusterAndSrcToDc("hetero-cluster", "jq", "oy");
        Assert.assertEquals(1, replication.getId());

    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/hetero-cluster-test.sql");
    }
}