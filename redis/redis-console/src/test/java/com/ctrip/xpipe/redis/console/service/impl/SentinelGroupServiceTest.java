package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SentinelGroupServiceTest extends AbstractServiceImplTest {

    @Autowired
    private SentinelGroupServiceImpl sentinelGroupService;

    @Autowired
    private SentinelServiceImpl sentinelService;

    @Autowired
    private ShardServiceImpl shardService;

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

    @Test
    public void find() {
        List<SentinelGroupModel> dcSentinels = sentinelGroupService.findAllByDcName("jq");
        Assert.assertEquals(dcSentinels.size(), 1);
        Assert.assertEquals(1L, dcSentinels.get(0).getSentinelGroupId());
        Assert.assertEquals(ClusterType.ONE_WAY.name(), dcSentinels.get(0).getClusterType().toUpperCase());
        Assert.assertEquals(3, dcSentinels.get(0).getSentinels().size());

        dcSentinels = sentinelGroupService.findAllByDcAndType("jq", ClusterType.SINGLE_DC);
        Assert.assertEquals(dcSentinels.size(), 0);

        dcSentinels = sentinelGroupService.findAllByDcAndType("oy", ClusterType.ONE_WAY);
        Assert.assertEquals(dcSentinels.size(), 1);
        Assert.assertEquals(2L, dcSentinels.get(0).getSentinelGroupId());
        Assert.assertEquals(ClusterType.ONE_WAY.name(), dcSentinels.get(0).getClusterType().toUpperCase());
        Assert.assertEquals(3, dcSentinels.get(0).getSentinels().size());

        SentinelGroupModel sentinelGroupModel = sentinelGroupService.findById(3);
        Assert.assertEquals(2, sentinelGroupModel.getSentinels().size());

    }

    @Test
    public void findByShard() {
        createCluster(ClusterType.ONE_WAY, Lists.newArrayList("one_way_shard_1", "one_way_shard_2"), "one_way_cluster");
        List<ShardTbl> clusterShards = shardService.findAllByClusterName("one_way_cluster");
        Map<Long, SentinelGroupModel> shardSentinels = sentinelGroupService.findByShard(clusterShards.get(0).getId());
        Assert.assertEquals(2, shardSentinels.size());
    }

    @Test
    public void addSentinelGroup() {
        SentinelGroupModel sentinelGroupModel1 = new SentinelGroupModel().setClusterType(ClusterType.SINGLE_DC.name()).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(6000),
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(6001),
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(6002)
        ));
        sentinelGroupService.addSentinelGroup(sentinelGroupModel1);

        List<SentinelGroupModel> singleDcSentinels = sentinelGroupService.findAllByDcAndType("jq", ClusterType.SINGLE_DC);
        Assert.assertEquals(1, singleDcSentinels.size());
        Assert.assertEquals(3, singleDcSentinels.get(0).getSentinels().size());
        Assert.assertEquals(ClusterType.SINGLE_DC.name(), singleDcSentinels.get(0).getClusterType());
        Assert.assertEquals(1, singleDcSentinels.get(0).dcIds().size());

        SentinelGroupModel sentinelGroupModel2 = new SentinelGroupModel().setClusterType(ClusterType.CROSS_DC.name()).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(7000),
                new SentinelInstanceModel().setDcId(2L).setSentinelIp("127.0.0.1").setSentinelPort(7001),
                new SentinelInstanceModel().setDcId(3L).setSentinelIp("127.0.0.1").setSentinelPort(7002)
        ));
        sentinelGroupService.addSentinelGroup(sentinelGroupModel2);
        List<SentinelGroupModel> crossDcSentinels = sentinelGroupService.findAllByDcAndType("jq", ClusterType.CROSS_DC);
        Assert.assertEquals(1, crossDcSentinels.size());
        Assert.assertEquals(1, crossDcSentinels.get(0).getSentinels().size());
        Assert.assertEquals(ClusterType.CROSS_DC.name(), crossDcSentinels.get(0).getClusterType());
        Assert.assertEquals(1, crossDcSentinels.get(0).dcIds().size());

        List<SentinelTbl> sentinelTbls = sentinelService.findBySentinelGroupId(1);
        sentinelTbls.forEach(sentinelTbl -> {
            sentinelService.delete(sentinelTbl.getSentinelId());
        });
        SentinelGroupModel sentinelGroupModel = sentinelGroupService.findById(1);
        Assert.assertEquals(0, sentinelGroupModel.getSentinels().size());

        sentinelGroupService.addSentinelGroup(new SentinelGroupModel().setSentinelGroupId(1).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(8000),
                new SentinelInstanceModel().setDcId(2L).setSentinelIp("127.0.0.1").setSentinelPort(8001),
                new SentinelInstanceModel().setDcId(3L).setSentinelIp("127.0.0.1").setSentinelPort(8002)
        )));
        sentinelGroupModel = sentinelGroupService.findById(1);
        Assert.assertEquals(3, sentinelGroupModel.getSentinels().size());
    }

    @Test
    public void getSentinelGroupsWithUsageByType() {
        ConsoleConfig consoleConfig = mock(ConsoleConfig.class);
        when(consoleConfig.supportSentinelHealthCheck(any(), anyString()))
                .thenReturn(true);
        shardService.setConsoleConfig(consoleConfig);
        SentinelGroupModel sentinelGroupModel1 = new SentinelGroupModel().setClusterType(ClusterType.ONE_WAY.name()).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(6000),
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(6001),
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(6002)
        ));
        sentinelGroupService.addSentinelGroup(sentinelGroupModel1);

        SentinelGroupModel sentinelGroupModelCrossDc1 = new SentinelGroupModel().setClusterType(ClusterType.CROSS_DC.name()).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(7000),
                new SentinelInstanceModel().setDcId(2L).setSentinelIp("127.0.0.1").setSentinelPort(7001),
                new SentinelInstanceModel().setDcId(2L).setSentinelIp("127.0.0.1").setSentinelPort(7002)
        ));
        sentinelGroupService.addSentinelGroup(sentinelGroupModelCrossDc1);

        SentinelGroupModel sentinelGroupModelCrossDc2 = new SentinelGroupModel().setClusterType(ClusterType.CROSS_DC.name()).setSentinels(Lists.newArrayList(
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(8000),
                new SentinelInstanceModel().setDcId(1L).setSentinelIp("127.0.0.1").setSentinelPort(8001),
                new SentinelInstanceModel().setDcId(2L).setSentinelIp("127.0.0.1").setSentinelPort(8002),
                new SentinelInstanceModel().setDcId(3L).setSentinelIp("127.0.0.1").setSentinelPort(8003),
                new SentinelInstanceModel().setDcId(3L).setSentinelIp("127.0.0.1").setSentinelPort(8004)
        ));
        sentinelGroupService.addSentinelGroup(sentinelGroupModelCrossDc2);

        //force refresh cache
        sentinelBalanceService.selectSentinelWithoutCache("OY", ClusterType.ONE_WAY);

        createCluster(ClusterType.ONE_WAY, Lists.newArrayList("one_way_shard_11", "one_way_shard_12", "one_way_shard_13"), "one_way_1");
        createCluster(ClusterType.ONE_WAY, Lists.newArrayList("one_way_shard_21", "one_way_shard_22"), "one_way_2");

        List<SentinelGroupModel> result = sentinelGroupService.getSentinelGroupsWithUsageByType(ClusterType.ONE_WAY);
        Assert.assertEquals(4, result.size());
        int jqUsageOneWay = 0;
        int oyUsageOneWay = 0;
        int fraUsageOneWay = 0;
        for (SentinelGroupModel sentinelGroupModel : result) {
            if (sentinelGroupModel.getClusterType().equalsIgnoreCase(ClusterType.ONE_WAY.name()) && sentinelGroupModel.dcIds().size() == 1) {
                long dcId = sentinelGroupModel.dcIds().iterator().next();
                if (dcId == 1L)
                    jqUsageOneWay += sentinelGroupModel.getShardCount();
                else if (dcId == 2L)
                    oyUsageOneWay += sentinelGroupModel.getShardCount();
                else
                    fraUsageOneWay += sentinelGroupModel.getShardCount();
            }
        }
        Assert.assertEquals(5, jqUsageOneWay);
        Assert.assertEquals(5, oyUsageOneWay);
        Assert.assertEquals(0, fraUsageOneWay);


        createCluster(ClusterType.CROSS_DC, Lists.newArrayList("cross_dc_shard_1", "cross_dc_shard_2"), "cross_dc_cluster");
        result = sentinelGroupService.getSentinelGroupsWithUsageByType(ClusterType.CROSS_DC);
        Assert.assertEquals(2, result.size());

        int crossDcUsage = 0;
        for (SentinelGroupModel sentinelGroupModel : result) {
            crossDcUsage += sentinelGroupModel.getShardCount();
        }
        Assert.assertEquals(2, crossDcUsage);


        List<SentinelGroupModel> all = sentinelGroupService.getAllSentinelGroupsWithUsage();
        Assert.assertEquals(6, all.size());
        int xpipeSentinels = 0;
        int xpipeSentinelsUsage = 0;
        int crossSentinels = 0;
        int crossSentinelsUsage = 0;
        for (SentinelGroupModel sentinelGroupModel : all) {
            if (sentinelGroupModel.getClusterType().equalsIgnoreCase(ClusterType.ONE_WAY.name())) {
                xpipeSentinels++;
                xpipeSentinelsUsage += sentinelGroupModel.getShardCount();
            } else if (sentinelGroupModel.getClusterType().equalsIgnoreCase(ClusterType.CROSS_DC.name())) {
                crossSentinels++;
                crossSentinelsUsage += sentinelGroupModel.getShardCount();
            }
        }
        Assert.assertEquals(4, xpipeSentinels);
        Assert.assertEquals(2, crossSentinels);
        Assert.assertEquals(10, xpipeSentinelsUsage);
        Assert.assertEquals(2, crossSentinelsUsage);


        Map<String, SentinelUsageModel> allUsages = sentinelGroupService.getAllSentinelsUsage(ClusterType.CROSS_DC.name());
        Assert.assertEquals(3, allUsages.size());
        Assert.assertEquals(2, allUsages.get("jq").getSentinelUsages().size());
        Assert.assertEquals(2, allUsages.get("oy").getSentinelUsages().size());
        Assert.assertEquals(1, allUsages.get("fra").getSentinelUsages().size());
    }

    @Test
    public void updateSentinelGroup() {
        List<SentinelGroupModel> dcSentinels = sentinelGroupService.findAllByDcName("jq");
        SentinelGroupModel toUpdate = dcSentinels.get(0);
        List<SentinelInstanceModel> instanceModels = toUpdate.getSentinels();
        SentinelInstanceModel updateInstance = instanceModels.get(0);
        String oldIp = updateInstance.getSentinelIp();
        int oldPort = updateInstance.getSentinelPort();

        Assert.assertTrue(toUpdate.getSentinelsAddressString().contains(String.format("%s:%d", oldIp, oldPort)));
        String newIp = "127.0.0.2";
        int newPort = 9999;
        updateInstance.setSentinelIp(newIp).setSentinelPort(newPort);

        sentinelGroupService.updateSentinelGroupAddress(toUpdate);
        SentinelGroupModel updated = sentinelGroupService.findById(toUpdate.getSentinelGroupId());

        Assert.assertFalse(updated.getSentinelsAddressString().contains(String.format("%s:%d", oldIp, oldPort)));
        Assert.assertTrue(updated.getSentinelsAddressString().contains(String.format("%s:%d", newIp, newPort)));

    }

    @Test
    public void deleteAndReheal() {
        List<SentinelGroupModel> dcSentinels = sentinelGroupService.findAllByDcName("jq");
        SentinelGroupModel toDelete = dcSentinels.get(0);

        sentinelGroupService.delete(toDelete.getSentinelGroupId());
        sentinelGroupService.findAllByDcName("jq").forEach(sentinelGroupModel -> {
            Assert.assertNotEquals(toDelete.getSentinelGroupId(),sentinelGroupModel.getSentinelGroupId());
        });

        sentinelGroupService.reheal(toDelete.getSentinelGroupId());
        Assert.assertNotNull(sentinelGroupService.findById(toDelete.getSentinelGroupId()));

        AtomicBoolean rehealed=new AtomicBoolean(false);
        sentinelGroupService.findAllByDcName("jq").forEach(sentinelGroupModel -> {
            if(toDelete.getSentinelGroupId()==sentinelGroupModel.getSentinelGroupId()){
                rehealed.set(true);
                Assert.assertTrue(sentinelGroupModel.getSentinels().size()>1);
            }
        });
        Assert.assertTrue(rehealed.get());
    }

}
