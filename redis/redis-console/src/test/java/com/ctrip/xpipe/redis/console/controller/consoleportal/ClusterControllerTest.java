package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterControllerTest extends AbstractConsoleIntegrationTest {
    @Autowired
    ClusterController controller;

    @Autowired
    ReplDirectionService replDirectionService;

    @Autowired
    ClusterService clusterService;

    @Autowired
    RedisService redisService;

    @Autowired
    DcClusterService dcClusterService;

    @Autowired
    DcClusterShardService dcClusterShardService;

    @Autowired
    ApplierService applierService;

    @Autowired
    ShardService shardService;

    @Test
    public void testUpdateReplDirection() {
        String heteroClusterName = "hetero-cluster";
        String wrongClusterName = "hetero-cluster2";
        long heteroClusterId = 7;
        ClusterTbl clusterTbl = clusterService.find(heteroClusterName);

        ReplDirectionTbl replDirectionTbl = replDirectionService.findReplDirectionTblById(1L);
        Assert.assertEquals(2, replDirectionTbl.getToDcId());
        replDirectionTbl = replDirectionService.findReplDirectionTblById(2L);
        Assert.assertEquals(3, replDirectionTbl.getToDcId());

        ReplDirectionInfoModel replDirectionInfoModel1 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("oy").setId(2L);
        ReplDirectionInfoModel replDirectionInfoModel2 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("fra").setId(1L);

        controller.updateClusterReplDirections(clusterTbl, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        replDirectionTbl = replDirectionService.findReplDirectionTblById(1L);
        Assert.assertEquals(3, replDirectionTbl.getToDcId());
        replDirectionTbl = replDirectionService.findReplDirectionTblById(2L);
        Assert.assertEquals(2, replDirectionTbl.getToDcId());

        try {
            controller.updateClusterReplDirections(null, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));
        } catch (Exception e) {
            Assert.assertEquals("[updateClusterReplDirections] cluster can not be null!", e.getMessage());
        }

        replDirectionInfoModel1.setClusterName(wrongClusterName);
        try {
            controller.updateClusterReplDirections(clusterTbl, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));
        } catch (Exception e) {
            Assert.assertEquals("[updateClusterReplDirections] repl direction should belong to cluster:7, but belong to cluster:8", e.getMessage());
        }

        replDirectionInfoModel1.setClusterName(heteroClusterName).setSrcDcName("oy");
        try {
            controller.updateClusterReplDirections(clusterTbl, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));
        } catch (Exception e) {
            Assert.assertEquals("[updateClusterReplDirections] repl direction should copy from src dc:1, but from 2", e.getMessage());
        }
    }

    @Test
    public void testUpdateHeteroCluster() {
        String shard1 = "hetero-cluster_1";
        String shard2 = "hetero-cluster_2";
        String shard4 = "hetero-cluster_3";
        String shard3 = "hetero-cluster_fra_1";
        String shard5 = "hetero-cluster_fra_2";

        ShardModel shardModel1 = new ShardModel();
        shardModel1.setShardTbl(new ShardTbl().setShardName(shard1).setSetinelMonitorName(shard1));
        ShardModel shardModel2 = new ShardModel();
        shardModel2.setShardTbl(new ShardTbl().setShardName(shard2).setSetinelMonitorName(shard2));
        ShardModel shardModel3 = new ShardModel();
        shardModel3.setShardTbl(new ShardTbl().setShardName(shard3).setSetinelMonitorName(shard3));
        ShardModel shardModel4 = new ShardModel();
        shardModel4.setShardTbl(new ShardTbl().setShardName(shard4).setSetinelMonitorName(shard4));
        ShardModel shardModel5 = new ShardModel();
        shardModel5.setShardTbl(new ShardTbl().setShardName(shard5).setSetinelMonitorName(shard5));

        String heteroClusterName = "hetero-cluster";
        long heteroClusterId = 7;
        ClusterTbl clusterTbl = clusterService.find(heteroClusterName);
        clusterTbl.setClusterAdminEmails("test@163.com");

        DcModel jq = new DcModel();
        jq.setDc_name("jq");
        DcClusterTbl jqDcClusterTbl = new DcClusterTbl().setClusterId(heteroClusterId).setDcId(1L).setGroupType(true).setGroupName("jq");
        DcClusterModel jqDcClusterModel = new DcClusterModel().setDc(jq).setDcCluster(jqDcClusterTbl).setShards(Lists.newArrayList(shardModel1, shardModel2));

        DcModel oy = new DcModel();
        oy.setDc_name("oy");
        DcClusterTbl oyDcClusterTbl = new DcClusterTbl().setClusterId(heteroClusterId).setDcId(2L).setGroupType(true).setGroupName("oy");
        DcClusterModel oyDcClusterModel = new DcClusterModel().setDc(oy).setDcCluster(oyDcClusterTbl).setShards(Lists.newArrayList(shardModel1, shardModel2));

        DcModel fra = new DcModel();
        fra.setDc_name("fra");
        DcClusterTbl fraDcClusterTbl = new DcClusterTbl().setClusterId(heteroClusterId).setDcId(3L).setGroupType(false).setGroupName("fra");
        DcClusterModel fraDcClusterModel = new DcClusterModel().setDc(fra).setDcCluster(fraDcClusterTbl).setShards(Lists.newArrayList(shardModel3));

        ReplDirectionInfoModel replDirectionInfoModel1 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("oy");
        ReplDirectionInfoModel replDirectionInfoModel2 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("fra");

        List<RedisTbl> redisTbls = redisService.findAllByDcClusterShard(53);
        Assert.assertEquals(4, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(54);
        Assert.assertEquals(4, redisTbls.size());

        redisTbls = redisService.findAllByDcClusterShard(51);
        Assert.assertEquals(6, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(52);
        Assert.assertEquals(6, redisTbls.size());

        // test delete dc oy and fra
        ClusterModel deleteOyFraModel = new ClusterModel();
        deleteOyFraModel.setClusterTbl(clusterTbl);
        deleteOyFraModel.setDcClusters(Lists.newArrayList(jqDcClusterModel));

        controller.updateCluster(heteroClusterName, deleteOyFraModel);

        DcClusterTbl dcClusterTbl = dcClusterService.find("oy", heteroClusterName);
        Assert.assertNull(dcClusterTbl);
        dcClusterTbl = dcClusterService.find("fra", heteroClusterName);
        Assert.assertNull(dcClusterTbl);
        dcClusterTbl = dcClusterService.find("jq", heteroClusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(31, dcClusterTbl.getDcClusterId());

        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(51, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertEquals(52, dcClusterShardTbl.getDcClusterShardId());

        //oy
        redisTbls = redisService.findAllByDcClusterShard(53);
        Assert.assertEquals(0, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(54);
        Assert.assertEquals(0, redisTbls.size());
        //fra
        redisTbls = redisService.findAllByDcClusterShard(55);
        Assert.assertEquals(0, redisTbls.size());
        //jq
        redisTbls = redisService.findAllByDcClusterShard(51);
        Assert.assertEquals(6, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(52);
        Assert.assertEquals(6, redisTbls.size());

        List<ApplierTbl> applierTbls = applierService.findApplierTblByShardAndReplDirection(21 , 2);
        Assert.assertEquals(0, applierTbls.size());
        applierTbls = applierService.findApplierTblByShardAndReplDirection(22 , 2);
        Assert.assertEquals(0, applierTbls.size());

        ShardTbl shardTbl = shardService.find(23);
        Assert.assertEquals(true, shardTbl.isDeleted());

        List<ReplDirectionTbl> allReplications = replDirectionService.findAllReplDirectionTblsByCluster(heteroClusterId);
        Assert.assertEquals(2, allReplications.size());

        // test add dc oy and fra
        // test add shard
        ClusterModel addOyFraModel = new ClusterModel();
        addOyFraModel.setClusterTbl(clusterTbl);
        shardModel1.setShardTbl(new ShardTbl().setShardName(shard1).setSetinelMonitorName(shard1));
        shardModel2.setShardTbl(new ShardTbl().setShardName(shard2).setSetinelMonitorName(shard2));
        shardModel3.setShardTbl(new ShardTbl().setShardName(shard3).setSetinelMonitorName(shard3));
        shardModel4.setShardTbl(new ShardTbl().setShardName(shard4).setSetinelMonitorName(shard4));
        shardModel5.setShardTbl(new ShardTbl().setShardName(shard5).setSetinelMonitorName(shard5));

        jqDcClusterModel.setShards(Lists.newArrayList(shardModel1, shardModel2, shardModel4));
        oyDcClusterModel.setShards(Lists.newArrayList(shardModel1, shardModel2, shardModel4));
        fraDcClusterModel.setShards(Lists.newArrayList(shardModel3, shardModel5));
        addOyFraModel.setDcClusters(Lists.newArrayList(jqDcClusterModel, oyDcClusterModel, fraDcClusterModel))
                .setReplDirections(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        controller.updateCluster(heteroClusterName, addOyFraModel);

        dcClusterTbl = dcClusterService.find("oy", heteroClusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertNotEquals(32, dcClusterTbl.getDcClusterId());
        dcClusterTbl = dcClusterService.find("fra", heteroClusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertNotEquals(33, dcClusterTbl.getDcClusterId());

        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(53, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(54, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard4);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(55, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard5);
        Assert.assertNotNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(51, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(52, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard4);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        allReplications = replDirectionService.findAllReplDirectionTblsByCluster(heteroClusterId);
        Assert.assertEquals(2, allReplications.size());

        // test delete shard
        ClusterModel deleteShardModel = new ClusterModel();
        deleteShardModel.setClusterTbl(clusterTbl);
        jqDcClusterModel.setShards(Lists.newArrayList(shardModel1));
        oyDcClusterModel.setShards(Lists.newArrayList(shardModel1));
        fraDcClusterModel.setShards(Lists.newArrayList(shardModel3));
        deleteShardModel.setDcClusters(Lists.newArrayList(jqDcClusterModel, oyDcClusterModel, fraDcClusterModel))
                .setReplDirections(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        controller.updateCluster(heteroClusterName, deleteShardModel);

        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(53, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(55, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(51, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        redisTbls = redisService.findAllByDcClusterShard(51);
        Assert.assertEquals(6, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(52);
        Assert.assertEquals(0, redisTbls.size());

        allReplications = replDirectionService.findAllReplDirectionTblsByCluster(heteroClusterId);
        Assert.assertEquals(2, allReplications.size());

        //test add shard
        shardModel1.setShardTbl(new ShardTbl().setShardName(shard1).setSetinelMonitorName(shard1));
        shardModel2.setShardTbl(new ShardTbl().setShardName(shard2).setSetinelMonitorName(shard2));
        shardModel3.setShardTbl(new ShardTbl().setShardName(shard3).setSetinelMonitorName(shard3));
        shardModel4.setShardTbl(new ShardTbl().setShardName(shard4).setSetinelMonitorName(shard4));
        shardModel5.setShardTbl(new ShardTbl().setShardName(shard5).setSetinelMonitorName(shard5));
        ClusterModel addShard = new ClusterModel();
        addShard.setClusterTbl(clusterTbl);
        jqDcClusterModel.setShards(Lists.newArrayList(shardModel1, shardModel2, shardModel4));
        oyDcClusterModel.setShards(Lists.newArrayList(shardModel1, shardModel2, shardModel4));
        fraDcClusterModel.setShards(Lists.newArrayList(shardModel3, shardModel5));
        addShard.setDcClusters(Lists.newArrayList(jqDcClusterModel, oyDcClusterModel, fraDcClusterModel))
                .setReplDirections(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        controller.updateCluster(heteroClusterName, addShard);

        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(53, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(54, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard4);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(55, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard5);
        Assert.assertNotNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(51, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(52, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard4);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);


        // test delete active dc
        ClusterModel deleteActiveDc = new ClusterModel();
        deleteActiveDc.setClusterTbl(clusterTbl);
        shardModel1.setShardTbl(new ShardTbl().setShardName(shard1).setSetinelMonitorName(shard1));
        shardModel2.setShardTbl(new ShardTbl().setShardName(shard2).setSetinelMonitorName(shard2));
        shardModel3.setShardTbl(new ShardTbl().setShardName(shard3).setSetinelMonitorName(shard3));
        shardModel4.setShardTbl(new ShardTbl().setShardName(shard4).setSetinelMonitorName(shard4));
        shardModel5.setShardTbl(new ShardTbl().setShardName(shard5).setSetinelMonitorName(shard5));
        oyDcClusterModel.setShards(Lists.newArrayList(shardModel1));
        fraDcClusterModel.setShards(Lists.newArrayList(shardModel3));
        deleteActiveDc.setDcClusters(Lists.newArrayList(oyDcClusterModel, fraDcClusterModel))
                .setReplDirections(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        try {
            controller.updateCluster(heteroClusterName, deleteActiveDc);
        } catch (Exception e) {
            Assert.assertEquals("can not unbind active dc", e.getMessage());
        }

    }

    @Test
    public void testUpdateHeteroCluster2() {
        String shard1 = "hetero-cluster_1";
        String shard2 = "hetero-cluster_oy_1";
        String shard4 = "hetero-cluster_oy_2";
        String shard3 = "hetero-cluster_fra_1";
        String shard5 = "hetero-cluster_fra_2";

        ShardModel shardModel1 = new ShardModel();
        shardModel1.setShardTbl(new ShardTbl().setShardName(shard1).setSetinelMonitorName(shard1));
        ShardModel shardModel2 = new ShardModel();
        shardModel2.setShardTbl(new ShardTbl().setShardName(shard2).setSetinelMonitorName(shard2));
        ShardModel shardModel3 = new ShardModel();
        shardModel3.setShardTbl(new ShardTbl().setShardName(shard3).setSetinelMonitorName(shard3));
        ShardModel shardModel4 = new ShardModel();
        shardModel4.setShardTbl(new ShardTbl().setShardName(shard4).setSetinelMonitorName(shard4));
        ShardModel shardModel5 = new ShardModel();
        shardModel5.setShardTbl(new ShardTbl().setShardName(shard5).setSetinelMonitorName(shard5));

        String heteroClusterName = "hetero-cluster";
        long heteroClusterId = 7;
        ClusterTbl clusterTbl = clusterService.find(heteroClusterName);
        clusterTbl.setClusterAdminEmails("test@163.com");

        List<RedisTbl> redisTbls = redisService.findAllByDcClusterShard(53);
        Assert.assertEquals(4, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(54);
        Assert.assertEquals(4, redisTbls.size());

        redisTbls = redisService.findAllByDcClusterShard(51);
        Assert.assertEquals(6, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(52);
        Assert.assertEquals(6, redisTbls.size());

        DcModel jq = new DcModel();
        jq.setDc_name("jq");
        DcClusterTbl jqDcClusterTbl = new DcClusterTbl().setClusterId(heteroClusterId).setDcId(1L).setGroupType(true).setGroupName("jq");
        DcClusterModel jqDcClusterModel = new DcClusterModel().setDc(jq).setDcCluster(jqDcClusterTbl).setShards(Lists.newArrayList(shardModel1));

        DcModel oy = new DcModel();
        oy.setDc_name("oy");
        DcClusterTbl oyDcClusterTbl = new DcClusterTbl().setClusterId(heteroClusterId).setDcId(2L).setGroupType(false).setGroupName("oy");
        DcClusterModel oyDcClusterModel = new DcClusterModel().setDc(oy).setDcCluster(oyDcClusterTbl).setShards(Lists.newArrayList(shardModel2, shardModel4));

        DcModel fra = new DcModel();
        fra.setDc_name("fra");
        DcClusterTbl fraDcClusterTbl = new DcClusterTbl().setClusterId(heteroClusterId).setDcId(3L).setGroupType(false).setGroupName("fra");
        DcClusterModel fraDcClusterModel = new DcClusterModel().setDc(fra).setDcCluster(fraDcClusterTbl).setShards(Lists.newArrayList(shardModel3, shardModel5));

        ReplDirectionInfoModel replDirectionInfoModel1 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("oy");
        ReplDirectionInfoModel replDirectionInfoModel2 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("fra");


        // test delete dc oy and fra
        ClusterModel deleteOyFraModel = new ClusterModel();
        deleteOyFraModel.setClusterTbl(clusterTbl);
        deleteOyFraModel.setDcClusters(Lists.newArrayList(jqDcClusterModel));

        controller.updateCluster(heteroClusterName, deleteOyFraModel);

        DcClusterTbl dcClusterTbl = dcClusterService.find("oy", heteroClusterName);
        Assert.assertNull(dcClusterTbl);
        dcClusterTbl = dcClusterService.find("fra", heteroClusterName);
        Assert.assertNull(dcClusterTbl);
        dcClusterTbl = dcClusterService.find("jq", heteroClusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(31, dcClusterTbl.getDcClusterId());
        ShardTbl shardTbl = shardService.find(23);
        Assert.assertEquals(true, shardTbl.isDeleted());


        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(51, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);

        //oy
        redisTbls = redisService.findAllByDcClusterShard(53);
        Assert.assertEquals(0, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(54);
        Assert.assertEquals(0, redisTbls.size());
        //fra
        redisTbls = redisService.findAllByDcClusterShard(55);
        Assert.assertEquals(0, redisTbls.size());
        //jq
        redisTbls = redisService.findAllByDcClusterShard(51);
        Assert.assertEquals(6, redisTbls.size());
        redisTbls = redisService.findAllByDcClusterShard(52);
        Assert.assertEquals(0, redisTbls.size());

        List<ApplierTbl> applierTbls = applierService.findApplierTblByShardAndReplDirection(21 , 2);
        Assert.assertEquals(0, applierTbls.size());
        applierTbls = applierService.findApplierTblByShardAndReplDirection(22 , 2);
        Assert.assertEquals(0, applierTbls.size());

        shardTbl = shardService.find(23);
        Assert.assertEquals(true, shardTbl.isDeleted());
        shardTbl = shardService.find(22);
        Assert.assertEquals(true, shardTbl.isDeleted());


        List<ReplDirectionTbl> allReplications = replDirectionService.findAllReplDirectionTblsByCluster(heteroClusterId);
        Assert.assertEquals(2, allReplications.size());

        // test add dc oy(master) and fra (master)
        ClusterModel addOyFraModel = new ClusterModel();
        addOyFraModel.setClusterTbl(clusterTbl);
        jqDcClusterModel.setShards(Lists.newArrayList(shardModel1));
        oyDcClusterModel.setShards(Lists.newArrayList(shardModel2, shardModel4));
        fraDcClusterModel.setShards(Lists.newArrayList(shardModel3, shardModel5));
        addOyFraModel.setDcClusters(Lists.newArrayList(jqDcClusterModel, oyDcClusterModel, fraDcClusterModel))
                .setReplDirections(Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        controller.updateCluster(heteroClusterName, addOyFraModel);

        dcClusterTbl = dcClusterService.find("oy", heteroClusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertNotEquals(32, dcClusterTbl.getDcClusterId());
        dcClusterTbl = dcClusterService.find("fra", heteroClusterName);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertNotEquals(33, dcClusterTbl.getDcClusterId());

        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard2);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard4);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard1);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard3);
        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertNotEquals(55, dcClusterShardTbl.getDcClusterShardId());
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("fra", heteroClusterName, shard5);
        Assert.assertNotNull(dcClusterShardTbl);

        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard1);
        Assert.assertNotNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard2);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard3);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("jq", heteroClusterName, shard4);
        Assert.assertNull(dcClusterShardTbl);
        dcClusterShardTbl = dcClusterShardService.find("oy", heteroClusterName, shard5);
        Assert.assertNull(dcClusterShardTbl);

        allReplications = replDirectionService.findAllReplDirectionTblsByCluster(heteroClusterId);
        Assert.assertEquals(2, allReplications.size());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/hetero-cluster-test.sql");
    }
}