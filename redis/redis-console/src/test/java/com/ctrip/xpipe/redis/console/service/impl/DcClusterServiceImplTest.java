package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.DcClusterModel;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jun 17, 2020
 */
public class DcClusterServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private DcClusterServiceImpl dcClusterService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test2.sql");
    }

    @Test
    public void testFind() {
        DcClusterTbl dcClusterTbl = dcClusterService.find(1L, 101L);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(101L, dcClusterTbl.getDcClusterId());
    }

    @Test
    public void testTestFind() {
        DcClusterTbl dcClusterTbl = dcClusterService.find("jq", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(101L, dcClusterTbl.getDcClusterId());
    }

    @Test
    public void testAddDcCluster() {
        dcClusterService.addDcCluster("fra", "cluster101");
        DcClusterTbl dcClusterTbl = dcClusterService.find("fra", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(101L, dcClusterTbl.getClusterId());
    }

    @Test(expected = BadRequestException.class)
    public void testAddDcClusterFailWithAlreadyExits() {
        try {
            dcClusterService.addDcCluster("jq", "cluster101");
        } catch (Exception e) {
            Assert.assertEquals(String.format("DcCluster dc:%s cluster:%s exist", "jq", "cluster101"), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testUpdateDcCluster() {
        dcClusterService.addDcCluster("fra", "cluster101");
        DcClusterTbl dcClusterTbl = dcClusterService.find("fra", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(null, dcClusterTbl.getActiveRedisCheckRules());

        dcClusterService.updateDcCluster(new DcClusterCreateInfo().setClusterName("cluster101").setDcName("fra").setRedisCheckRule("0,1"));
        dcClusterTbl = dcClusterService.find("fra", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals("0,1", dcClusterTbl.getActiveRedisCheckRules());

        dcClusterService.updateDcCluster(new DcClusterCreateInfo().setClusterName("cluster101").setDcName("fra"));
        dcClusterTbl = dcClusterService.find("fra", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(null, dcClusterTbl.getActiveRedisCheckRules());
    }

    @Test (expected = BadRequestException.class)
    public void testUpdateDcClusterFail() {
        DcClusterCreateInfo dcClusterCreateInfo = new DcClusterCreateInfo().setClusterName("cluster101").setDcName("fra").setRedisCheckRule("0,1");
        try {
            dcClusterService.updateDcCluster(dcClusterCreateInfo);
        } catch (Exception e) {
            Assert.assertEquals(String.format("Can not update unexist dcCluster %s:%s",
                    dcClusterCreateInfo.getDcName(), dcClusterCreateInfo.getClusterName()), e.getMessage());
            throw e;
        }
    }

    @Test
    public void testFindAllDcClusterByClusterName() {
        List<DcClusterCreateInfo> dcCluster101s = dcClusterService.findClusterRelated("cluster101");
        Assert.assertEquals(2, dcCluster101s.size());

        dcClusterService.addDcCluster("fra", "cluster101", "0,1");

        dcCluster101s = dcClusterService.findClusterRelated("cluster101");
        dcCluster101s.forEach(dcCluster101-> {
            logger.info(dcCluster101.toString());
        });
        Assert.assertEquals(3, dcCluster101s.size());
    }


    @Test
    public void testFindAllDcClusters() {
        dcClusterService.findAllDcClusters();
    }

    @Test
    public void testFindByClusterIds() {
        List<DcClusterTbl> result = dcClusterService.findByClusterIds(Lists.newArrayList(101L));
        Assert.assertNotNull(result);
        Assert.assertEquals(101L, result.get(0).getClusterId());
    }

    @Test
    public void testFindAllByDcId() {
    }

    @Test
    public void testFindClusterRelated() {
        List<DcClusterTbl> dcClusterTbls = dcClusterService.findClusterRelated(101L);
        Assert.assertEquals(2, dcClusterTbls.size());
    }

    @Test
    public void testFindDcClusterModel() {
        DcClusterModel fra = dcClusterService.findDcClusterModelByClusterAndDc("hetero-cluster", "fra");

        Assert.assertEquals(1, fra.getShards().size());
        Assert.assertEquals(2, fra.getShards().get(0).getRedises().size());
        Assert.assertEquals(0, fra.getShards().get(0).getKeepers().size());

        Assert.assertEquals(1, fra.getSources().size());
        Assert.assertEquals(2, fra.getSources().get(0).getShards().size());
        Assert.assertEquals(2, fra.getSources().get(0).getShards().get(0).getAppliers().size());
        Assert.assertEquals(2, fra.getSources().get(0).getShards().get(1).getAppliers().size());

        DcClusterModel jq = dcClusterService.findDcClusterModelByClusterAndDc("hetero-cluster", "jq");
        Assert.assertEquals(2, jq.getShards().size());
        Assert.assertEquals(2, jq.getShards().get(0).getRedises().size());
        Assert.assertEquals(2, jq.getShards().get(0).getKeepers().size());
        Assert.assertEquals(2, jq.getShards().get(1).getRedises().size());
        Assert.assertEquals(2, jq.getShards().get(1).getKeepers().size());

        Assert.assertNull(jq.getSources());
    }

    @Test
    public void testFindDcClusterModelByCluster() {
        List<DcClusterModel> dcClusterModels = dcClusterService.findDcClusterModelsByCluster("hetero-cluster");
        Assert.assertEquals(3, dcClusterModels.size());

        try {
            dcClusterModels = dcClusterService.findDcClusterModelsByCluster("none");
        } catch (Exception e) {
            Assert.assertEquals(String.format("cluster %s does not exist", "none"), e.getMessage());
        }

    }
}
