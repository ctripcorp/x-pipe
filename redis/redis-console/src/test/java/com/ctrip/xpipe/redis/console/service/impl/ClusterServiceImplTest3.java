package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.entity.AzGroupClusterEntity;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.repository.AzGroupClusterRepository;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class ClusterServiceImplTest3 extends AbstractConsoleIntegrationTest{


    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private AzGroupClusterRepository azGroupClusterRepository;

    @Before
    public void beforeStart(){
        MockitoAnnotations.initMocks(this);
    }

    @Override
    public String prepareDatas(){
        try{
            return prepareDatasFromFile("src/test/resources/cluster-service-impl-test.sql");
        }catch (Exception e){
            logger.error("[ClusterServiceImplTest3]prepare data error for path", e);
        }
        return "";
    }

    @Test
    public void testFindAllClusterByDcNameBind(){
        List<String> dcNameList = new LinkedList<>();
        dcNameList.add("jq");
        dcNameList.add("oy");
        dcNameList.add("fra");
        dcNameList.forEach(dcName->{
            List<ClusterTbl> result =  clusterService.findAllClusterByDcNameBind(dcName);
            if (dcName.equals("jq")){
                Assert.assertEquals(2, result.size());
            }else if (dcName.equals("oy")){
                Assert.assertEquals(2, result.size());
            }else if (dcName.equals("fra")){
                Assert.assertEquals(0, result.size());
            }
        });

    }

    @Test
    public void testBindDC() {
        clusterService.bindDc(new DcClusterTbl().setClusterName("cluster7").setDcName("jq").setGroupType(DcGroupType.DR_MASTER.toString()));
        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("jq", "cluster7", "shard1");

        Assert.assertNull(dcClusterShardTbl);

        DcClusterTbl dcClusterTbl = dcClusterService.find("jq", "cluster7");
        Assert.assertEquals(DcGroupType.DR_MASTER.toString(), dcClusterTbl.getGroupType());
    }

    @Test
    public void testBindDcWithExistDrMasterDc() {
        clusterService.bindDc(new DcClusterTbl().setClusterName("cluster7").setDcName("jq").setGroupType(DcGroupType.DR_MASTER.toString()));

        ShardTbl shardTbl = shardService.find("cluster7", "shard1");

        DcClusterTbl dcClusterTbl = dcClusterService.find("jq", "cluster7");
        DcClusterShardTbl proto = new DcClusterShardTbl().setShardId(shardTbl.getId()).setDcClusterId(dcClusterTbl.getDcClusterId());
        dcClusterShardService.insertBatch(Lists.newArrayList(proto));

        clusterService.bindDc(new DcClusterTbl().setClusterName("cluster7").setDcName("oy").setGroupType(DcGroupType.DR_MASTER.toString()));
        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("oy", "cluster7", "shard1");
        Assert.assertNotNull(dcClusterShardTbl);
    }

    @Test
    public void testBindMasterDc() {
        clusterService.bindDc(new DcClusterTbl().setClusterName("cluster7").setDcName("oy").setGroupType(DcGroupType.MASTER.toString()));

        DcClusterTbl dcClusterTbl = dcClusterService.find("oy", "cluster7");
        Assert.assertNotEquals(DcGroupType.DR_MASTER.toString(), dcClusterTbl.getGroupType());

        // master dc will not create dcClusterShard automatically
        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("oy", "cluster7", "shard1");
        Assert.assertNull(dcClusterShardTbl);
    }

    @Test
    public void testUnBindMasterDc() {
        AzGroupClusterEntity azGroupCluster = new AzGroupClusterEntity().setClusterId(7L).setAzGroupId(4L).setActiveAzId(2L).setAzGroupClusterType("SINGLE_DC");
        azGroupClusterRepository.insert(azGroupCluster);
        clusterService.bindRegionAz("cluster7", "SHA", "oy");

        DcClusterTbl dcClusterTbl = dcClusterService.find("oy", "cluster7");

        shardService.findOrCreateShardIfNotExist("cluster7", new ShardTbl().setShardName("shard3"), Lists.newArrayList(dcClusterTbl), null);

        ClusterTbl clusterTbl = clusterService.find("cluster7");
        List<ShardTbl> shards = shardService.findAllShardByDcCluster(dcClusterTbl.getDcId(), clusterTbl.getId());
        Assert.assertEquals(1, shards.size());
        Assert.assertEquals("shard3", shards.get(0).getShardName());

        clusterService.unbindDc("cluster7", "oy");
        DcClusterTbl dcClusterTbl1 = dcClusterService.find("oy", "cluster7");
        Assert.assertNull(dcClusterTbl1);
        List<ShardTbl> shard2AfterUnbind = shardService.findAllByClusterName("cluster7");
        Assert.assertFalse(shard2AfterUnbind.stream().anyMatch(shardTbl -> "shard3".equals(shardTbl.getShardName())));
    }

    @Test
    public void testDivideClusters() {
        List<Set<String>> clusterParts = clusterService.divideClusters(3);
        Assert.assertEquals(3, clusterParts.size());
        Assert.assertEquals(Sets.newHashSet("cluster3"), clusterParts.get(0));
        Assert.assertEquals(Sets.newHashSet("cluster1", "cluster4", "cluster7"), clusterParts.get(1));
        Assert.assertEquals(Sets.newHashSet("cluster5"), clusterParts.get(2));
    }

}
