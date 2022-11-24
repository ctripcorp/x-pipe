package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ApplierService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.redis.console.service.model.SourceModelService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;


public class ClusterServiceImplTest4 extends AbstractConsoleIntegrationTest {


    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ReplDirectionService replDirectionService;

    @Autowired
    private ShardModelService shardModelService;

    @Autowired
    private ApplierService applierService;

    @Autowired
    private SourceModelService sourceModelService;


    @Override
    public String prepareDatas() {
        try {
            return prepareDatasFromFile("src/test/resources/cluster-service-impl-test4.sql");
        } catch (Exception e) {
            logger.error("[ClusterServiceImplTest3]prepare data error for path", e);
        }
        return "";
    }

    @Test
    public void testCompleteReplicationFailWhenToDcIsDrMaster() {
        String clusterName = "repl-hetero-cluster1";
        int replDirectionId = 11;
        final ClusterTbl clusterTbl = clusterService.find(clusterName);
        final ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(replDirectionId);
        try {
            clusterService.completeReplicationByClusterAndReplDirection(clusterTbl, replDirection);
        } catch (Throwable th) {
            Assert.assertEquals("to dc oy is Dr master Dc in cluster repl-hetero-cluster1", th.getMessage());
        }

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(41, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(42, replDirectionId);
        Assert.assertEquals(0, appliers.size());

        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster1_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster1_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
    }

    @Test
    public void testCompleteReplicationSuccessWithNoTransitNode() {
        String clusterName = "repl-hetero-cluster1";
        int replDirectionId = 12;
        final ClusterTbl clusterTbl = clusterService.find(clusterName);

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(41, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(42, replDirectionId);
        Assert.assertEquals(0, appliers.size());

        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster1_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster1_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        final ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(replDirectionId);

        clusterService.completeReplicationByClusterAndReplDirection(clusterTbl, replDirection);

        appliers = applierService.findApplierTblByShardAndReplDirection(41, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(42, replDirectionId);
        Assert.assertEquals(2, appliers.size());

        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster1_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster1_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
    }

    @Test
    public void testCompleteReplicationSuccessWithPureTransitNode() {
        String clusterName = "repl-hetero-cluster2";
        int replDirectionId = 14;
        final ClusterTbl clusterTbl = clusterService.find(clusterName);
        final ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(replDirectionId);

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(51, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(52, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(53, replDirectionId);
        Assert.assertEquals(0, appliers.size());

        List<SourceModel> oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach((sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(0, shardModel.getKeepers().size());
            });
        }));

        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster2_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster2_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster2_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        clusterService.completeReplicationByClusterAndReplDirection(clusterTbl, replDirection);

        appliers = applierService.findApplierTblByShardAndReplDirection(51, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(52, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(53, replDirectionId);
        Assert.assertEquals(2, appliers.size());

        oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach((sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        }));

        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster2_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster2_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster2_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
    }

    @Test
    public void testCompleteReplicationSuccessWithNonPureTransitNode() {
        String clusterName = "repl-hetero-cluster3";
        int replDirectionId = 16;
        final ClusterTbl clusterTbl = clusterService.find(clusterName);
        final ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(replDirectionId);

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(61, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(62, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(63, replDirectionId);
        Assert.assertEquals(0, appliers.size());

        appliers = applierService.findApplierTblByShardAndReplDirection(66, replDirectionId);
        Assert.assertEquals(0, appliers.size());

        List<SourceModel> oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach((sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(0, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        }));

        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster3_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster3_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster3_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        shard = shardModelService.getShardModel("oy", clusterName, "repl-hetero-cluster3_oy_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        clusterService.completeReplicationByClusterAndReplDirection(clusterTbl, replDirection);

        appliers = applierService.findApplierTblByShardAndReplDirection(61, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(62, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(63, replDirectionId);
        Assert.assertEquals(2, appliers.size());

        appliers = applierService.findApplierTblByShardAndReplDirection(66, replDirectionId);
        Assert.assertEquals(0, appliers.size());

        oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach((sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        }));

        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster3_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster3_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster3_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        shard = shardModelService.getShardModel("oy", clusterName, "repl-hetero-cluster3_oy_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
    }

    @Test
    public void testCompleteReplicationSuccessWithPureChainedTransitNode() {
        String clusterName = "repl-hetero-cluster4";
        int replDirectionId = 19;
        int upstreamReplDirectionId = 18;
        final ClusterTbl clusterTbl = clusterService.find(clusterName);
        final ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(replDirectionId);

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(71, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(72, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(73, replDirectionId);
        Assert.assertEquals(0, appliers.size());


        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster4_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster4_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster4_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        clusterService.completeReplicationByClusterAndReplDirection(clusterTbl, replDirection);

        appliers = applierService.findApplierTblByShardAndReplDirection(71, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(72, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(73, replDirectionId);
        Assert.assertEquals(2, appliers.size());

        List<SourceModel> rb = sourceModelService.getAllSourceModels("rb", clusterName);
        rb.forEach(sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });

        List<SourceModel> oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach(sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });

        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster4_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster4_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster4_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
    }

    @Test
    public void testCompleteReplicationSuccessWithNonPureChainedTransitNode() {
        String clusterName = "repl-hetero-cluster5";
        int replDirectionId = 23;
        int upstreamReplDirectionId = 22;
        final ClusterTbl clusterTbl = clusterService.find(clusterName);
        final ReplDirectionInfoModel replDirection = replDirectionService.findReplDirectionInfoModelById(replDirectionId);

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(81, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, replDirectionId);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, replDirectionId);
        Assert.assertEquals(0, appliers.size());


        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        clusterService.completeReplicationByClusterAndReplDirection(clusterTbl, replDirection);

        appliers = applierService.findApplierTblByShardAndReplDirection(81, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, replDirectionId);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, replDirectionId);
        Assert.assertEquals(2, appliers.size());

        List<SourceModel> rb = sourceModelService.getAllSourceModels("rb", clusterName);
        rb.forEach(sourceModel -> {
            Assert.assertEquals(3, sourceModel.getShards().size());
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });

        List<SourceModel> oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach(sourceModel -> {
            Assert.assertEquals(3, sourceModel.getShards().size());
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });

        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
    }

    @Test
    public void testCompleteReplicationSuccessByCluster() {
        String clusterName = "repl-hetero-cluster5";
        ClusterTbl cluster = clusterService.find(clusterName);
        List<ReplDirectionInfoModel> replDirections = replDirectionService.findAllReplDirectionInfoModelsByCluster(clusterName);

        List<ApplierTbl> appliers = applierService.findApplierTblByShardAndReplDirection(81, 23);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, 23);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, 23);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(86, 23);
        Assert.assertEquals(0, appliers.size());

        appliers = applierService.findApplierTblByShardAndReplDirection(81, 22);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, 22);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, 22);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(87, 22);
        Assert.assertEquals(0, appliers.size());

        appliers = applierService.findApplierTblByShardAndReplDirection(81, 21);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, 21);
        Assert.assertEquals(0, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, 21);
        Assert.assertEquals(0, appliers.size());

        ShardModel shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        shard = shardModelService.getShardModel("oy", clusterName, "repl-hetero-cluster5_oy_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        shard = shardModelService.getShardModel("rb", clusterName, "repl-hetero-cluster5_rb_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        shard = shardModelService.getShardModel("fra", clusterName, "repl-hetero-cluster5_fra_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("fra", clusterName, "repl-hetero-cluster5_fra_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        List<SourceModel> oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach(sourceModel -> {
            Assert.assertEquals(3, sourceModel.getShards().size());
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(0, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });

        List<SourceModel> rb = sourceModelService.getAllSourceModels("rb", clusterName);
        rb.forEach(sourceModel -> {
            Assert.assertEquals(3, sourceModel.getShards().size());
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(0, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });
        List<SourceModel> fra = sourceModelService.getAllSourceModels("fra", clusterName);
        fra.forEach(sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(0, shardModel.getKeepers().size());
                Assert.assertEquals(0, shardModel.getAppliers().size());
            });
        });

        replDirections.forEach(replDirection -> clusterService.completeReplicationByClusterAndReplDirection(cluster, replDirection));


        appliers = applierService.findApplierTblByShardAndReplDirection(81, 23);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, 23);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, 23);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(86, 24);
        Assert.assertEquals(2, appliers.size());

        appliers = applierService.findApplierTblByShardAndReplDirection(81, 22);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, 22);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, 22);
        Assert.assertEquals(2, appliers.size());

        appliers = applierService.findApplierTblByShardAndReplDirection(81, 21);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(82, 21);
        Assert.assertEquals(2, appliers.size());
        appliers = applierService.findApplierTblByShardAndReplDirection(83, 21);
        Assert.assertEquals(2, appliers.size());

        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("jq", clusterName, "repl-hetero-cluster5_3", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        shard = shardModelService.getShardModel("oy", clusterName, "repl-hetero-cluster5_oy_1", false, null);
        Assert.assertEquals(2, shard.getKeepers().size());

        shard = shardModelService.getShardModel("fra", clusterName, "repl-hetero-cluster5_fra_1", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());
        shard = shardModelService.getShardModel("fra", clusterName, "repl-hetero-cluster5_fra_2", false, null);
        Assert.assertEquals(0, shard.getKeepers().size());

        oy = sourceModelService.getAllSourceModels("oy", clusterName);
        oy.forEach(sourceModel -> {
            Assert.assertEquals(3, sourceModel.getShards().size());
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(2, shardModel.getAppliers().size());
            });
        });

        rb = sourceModelService.getAllSourceModels("rb", clusterName);
        rb.forEach(sourceModel -> {
            Assert.assertEquals(3, sourceModel.getShards().size());
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(2, shardModel.getKeepers().size());
                Assert.assertEquals(2, shardModel.getAppliers().size());
            });
        });
        fra = sourceModelService.getAllSourceModels("fra", clusterName);
        fra.forEach(sourceModel -> {
            sourceModel.getShards().forEach(shardModel -> {
                Assert.assertEquals(0, shardModel.getKeepers().size());
                Assert.assertEquals(2, shardModel.getAppliers().size());
            });
        });
    }
}
