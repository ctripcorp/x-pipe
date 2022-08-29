package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
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
        clusterService.bindDc(new DcClusterTbl().setClusterName("cluster7").setDcName("jq").setGroupType(true));
        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find("jq", "cluster7", "shard1");

        Assert.assertNotNull(dcClusterShardTbl);
        Assert.assertEquals(1, dcClusterShardTbl.getSetinelId());

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
