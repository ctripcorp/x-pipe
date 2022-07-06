package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import com.ctrip.xpipe.redis.console.model.AppliercontainerTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.ApplierBasicInfo;
import com.ctrip.xpipe.redis.console.service.ApplierService;
import com.ctrip.xpipe.redis.console.service.AppliercontainerService;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class ApplierServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    private ApplierService applierService;

    @Autowired
    private ShardModelService shardModelService;

    @Autowired
    AppliercontainerService appliercontainerService;

    @Test
    public void testFind() {
        ApplierTbl applierTbl = applierService.findApplierTblById(1);
        Assert.assertNotNull(applierTbl);
        Assert.assertEquals("127.0.0.1", applierTbl.getIp());

        applierTbl = applierService.findApplierTblById(12);
        Assert.assertNull(applierTbl);

        applierTbl = applierService.findApplierTblByIpPort("127.0.0.1", 16000);
        Assert.assertNotNull(applierTbl);
        Assert.assertEquals(1, applierTbl.getId());

        applierTbl = applierService.findApplierTblByIpPort("127.0.0.1", 16004);
        Assert.assertNull(applierTbl);

        List<ApplierTbl> applierTbls = applierService.findApplierTblByShardAndReplDirection(24, 4);
        Assert.assertEquals(2, applierTbls.size());

        applierTbls = applierService.findApplierTblByShardAndReplDirection(24, 7);
        Assert.assertEquals(0, applierTbls.size());

        applierTbls = applierService.findAllApplierTblsWithSameIp("127.0.0.1");
        Assert.assertEquals(3, applierTbls.size());

        applierTbls = applierService.findAllApplierTblsWithSameIp("127.0.0.10");
        Assert.assertEquals(0, applierTbls.size());
    }

    @Test
    public void testUpdateAppliersSuccess() {
        ShardModel sourceShardModel = shardModelService.getSourceShardModel("hetero-cluster", "jq", "fra", "hetero-cluster_1");
        Assert.assertEquals(2, sourceShardModel.getAppliers().size());
        Assert.assertEquals(16000, sourceShardModel.getAppliers().get(0).getPort());
        Assert.assertEquals(16000, sourceShardModel.getAppliers().get(1).getPort());

        ApplierTbl newApplier1 = new ApplierTbl().setContainerId(3).setActive(true).setPort(16055).setIp("127.0.0.3")
                .setShardId(21).setReplDirectionId(2);
        ApplierTbl newApplier2 = new ApplierTbl().setContainerId(4).setActive(true).setPort(16055).setIp("127.0.0.4")
                .setShardId(21).setReplDirectionId(2);
        List<ApplierTbl> newAppliers = Lists.newArrayList(newApplier1, newApplier2);

        ShardModel newSourceShardModel = new ShardModel();
        newSourceShardModel.setShardTbl(sourceShardModel.getShardTbl());
        newSourceShardModel.addApplier(newApplier1).addApplier(newApplier2);

        applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        sourceShardModel = shardModelService.getSourceShardModel("hetero-cluster", "jq", "fra", "hetero-cluster_1");
        Assert.assertEquals(2, sourceShardModel.getAppliers().size());
        Assert.assertEquals(16055, sourceShardModel.getAppliers().get(0).getPort());
        Assert.assertEquals(16055, sourceShardModel.getAppliers().get(1).getPort());

        ShardModel newSourceShardModel2 = new ShardModel();
        newSourceShardModel2.setShardTbl(sourceShardModel.getShardTbl());
        applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel2, 2L);
        sourceShardModel = shardModelService.getSourceShardModel("hetero-cluster", "jq", "fra", "hetero-cluster_1");
        Assert.assertEquals(0, sourceShardModel.getAppliers().size());

    }

    @Test
    public void testUpdateAppliersFail() {
        ShardModel sourceShardModel = shardModelService.getSourceShardModel("hetero-cluster", "jq", "fra", "hetero-cluster_1");
        Assert.assertEquals(2, sourceShardModel.getAppliers().size());
        Assert.assertEquals(16000, sourceShardModel.getAppliers().get(0).getPort());
        Assert.assertEquals(16000, sourceShardModel.getAppliers().get(1).getPort());
        ApplierTbl newApplier1 = new ApplierTbl().setContainerId(2).setActive(true).setPort(16005).setIp("127.0.0.2")
                .setShardId(21).setReplDirectionId(2);
        ApplierTbl newApplier2 = new ApplierTbl().setContainerId(4).setActive(true).setPort(16005).setIp("127.0.0.4")
                .setShardId(21).setReplDirectionId(2);
        List<ApplierTbl> newAppliers = Lists.newArrayList(newApplier1, newApplier2);

        ShardModel newSourceShardModel = new ShardModel();
        newSourceShardModel.setShardTbl(sourceShardModel.getShardTbl());
        newSourceShardModel.addApplier(newApplier1);
        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        } catch (Exception e) {
            Assert.assertEquals("size of appliers must be 0 or 2", e.getMessage());
        }

        newSourceShardModel.addApplier(newApplier2);
        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        } catch (Exception e) {
            Assert.assertEquals("If you wanna change applier port in same applier container, please delete it first!!", e.getMessage());
        }

        newSourceShardModel.getAppliers().get(0).setContainerId(3).setPort(16000).setIp("127.0.0.3");
        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        } catch (Exception e) {
            Assert.assertEquals("Already int use for applier`s port:16000", e.getMessage());
        }

        newSourceShardModel.getAppliers().get(0).setContainerId(3).setPort(16000).setIp("127.0.0.7");
        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        } catch (Exception e) {
            Assert.assertEquals("applier's ip : 127.0.0.7 should be equal to applier container's ip : 127.0.0.3", e.getMessage());
        }

        newSourceShardModel.getAppliers().get(0).setContainerId(17);
        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        } catch (Exception e) {
            Assert.assertEquals("can not find related applier containers 17", e.getMessage());
        }

        newSourceShardModel.getAppliers().get(0).setContainerId(4);
        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", newSourceShardModel, 2L);
        } catch (Exception e) {
            Assert.assertEquals("appliers should be assigned to different applier containers : " + newSourceShardModel.getAppliers(), e.getMessage());
        }

        try {
            applierService.updateAppliers("fra", "hetero-cluster", "hetero-cluster_1", null, 2L);
        } catch (Exception e) {
            Assert.assertEquals("[updateAppliers]sourceModel can not be null", e.getMessage());
        }

    }

    @Test
    public void findBestAppliers() {
        List<ApplierBasicInfo> appliers = applierService.findBestAppliers("fra", 16000, (ip, port) -> { return true; },  null);
        Assert.assertEquals(2, appliers.size());
        AppliercontainerTbl appliercontainer1 = appliercontainerService.findAppliercontainerTblById(appliers.get(0).getAppliercontainerId());
        AppliercontainerTbl appliercontainer2 = appliercontainerService.findAppliercontainerTblById(appliers.get(1).getAppliercontainerId());
        Assert.assertEquals(true, appliercontainer1.getAppliercontainerAz() != appliercontainer2.getAppliercontainerAz());
        List<ApplierTbl> existAppliers = applierService.findAllApplierTblsWithSameIp(appliers.get(0).getHost());
        existAppliers.forEach( existApplier -> {
            Assert.assertNotEquals(appliers.get(0).getPort(), existApplier.getPort());
        });

        existAppliers = applierService.findAllApplierTblsWithSameIp(appliers.get(1).getHost());
        existAppliers.forEach( existApplier -> {
            Assert.assertNotEquals(appliers.get(1).getPort(), existApplier.getPort());
        });
    }
    @Test
    public void findBestAppliersFail() {
        try {
            List<ApplierBasicInfo> appliers = applierService.findBestAppliers("jq", 16000, (ip, port) -> { return true; },  null);
        } catch (Exception e) {
            Assert.assertEquals("find appliercontainer size:1, but we need:2", e.getMessage());
        }
    }

    @Test
    public void testFindAllAppliercontainerCountInfo() {
        applierService.findAllAppliercontainerCountInfo();
    }


    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/hetero-cluster-test.sql");
    }
}