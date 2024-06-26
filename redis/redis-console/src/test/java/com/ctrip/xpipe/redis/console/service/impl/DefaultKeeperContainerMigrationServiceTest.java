package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardKeeper;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yu
 * <p>
 * 2023/9/20
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperContainerMigrationServiceTest {

    @InjectMocks
    private DefaultKeeperContainerMigrationService service;

    @Mock
    private ShardModelService shardModelService;

    @Before
    public void before() {
        ShardModel shardModel = new ShardModel();
        Mockito.when(shardModelService.getShardModel(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),  Mockito.anyObject()))
                .thenReturn(shardModel);
        Mockito.when(shardModelService.migrateBackupKeeper(Mockito.anyString(), Mockito.anyString(),  Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(true);
        Mockito.when(shardModelService.switchActiveKeeper(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(true);
    }

    @Test
    public void testMigrationKeeperContainer() throws Throwable {
        List<MigrationKeeperContainerDetailModel> models = new ArrayList<>();

        MigrationKeeperContainerDetailModel model = new MigrationKeeperContainerDetailModel();
        KeeperContainerUsedInfoModel src = new KeeperContainerUsedInfoModel()
                        .setKeeperIp("1.1.1.1").setDcName("jq").setTotalInputFlow(300 * 1024 * 1024L)
                        .setTotalRedisUsedMemory(500 * 1024 * 1024 * 1024L);
        Map<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> detailInfo = Maps.newHashMap();
        detailInfo.put(new DcClusterShardKeeper("jq", "cluster1", "shard1", true), new KeeperContainerUsedInfoModel.KeeperUsedInfo(200 * 1024 * 1024L, 400 * 1024 * 1024L, ""));
        detailInfo.put(new DcClusterShardKeeper("jq", "cluster1", "shard2", true), new KeeperContainerUsedInfoModel.KeeperUsedInfo(20 * 1024 * 1024L, 20 * 1024 * 1024L, ""));
        detailInfo.put(new DcClusterShardKeeper("jq", "cluster2", "shard1", true), new KeeperContainerUsedInfoModel.KeeperUsedInfo(30 * 1024 * 1024L, 30 * 1024 * 1024L, ""));
        detailInfo.put(new DcClusterShardKeeper("jq", "cluster2", "shard2", true), new KeeperContainerUsedInfoModel.KeeperUsedInfo(40 * 1024 * 1024L, 40 * 1024 * 1024L, ""));
        src.setDetailInfo(detailInfo);

        KeeperContainerUsedInfoModel target = new KeeperContainerUsedInfoModel()
                        .setKeeperIp("2.2.2.2").setDcName("jq").setTotalInputFlow(300 * 1024 * 1024L)
                        .setTotalRedisUsedMemory(500 * 1024 * 1024 * 1024L);
        Map<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardKeeper("jq", "cluster1", "shard1", true), new KeeperContainerUsedInfoModel.KeeperUsedInfo(200 * 1024 * 1024L, 400 * 1024 * 1024L, ""));
        target.setDetailInfo(detailInfo2);

        List<DcClusterShard> migrationShards = new ArrayList<>();
        migrationShards.add(new DcClusterShard("jq", "cluster1", "shard2"));
        migrationShards.add(new DcClusterShard("jq", "cluster2", "shard1"));
        migrationShards.add(new DcClusterShard("jq", "cluster2", "shard2"));

        model.setSrcKeeperContainer(src).setTargetKeeperContainer(target).setMigrateKeeperCount(3).setMigrateShards(migrationShards);
        models.add(model);
        service.beginMigrateKeeperContainers(models);
        Assert.assertEquals(0, service.getMigrationProcess().get(0).getMigrateKeeperCompleteCount());

        models.clear();
        model.setSwitchActive(true);
        models.add(model);
        service.beginMigrateKeeperContainers(models);
        Assert.assertEquals(3, service.getMigrationProcess().get(0).getMigrateKeeperCompleteCount());
        Assert.assertEquals(3, service.getMigrationProcess().get(0).getFinishedShards().size());

        models.clear();
        model.setSwitchActive(false);
        model.setKeeperPairOverload(true);
        models.add(model);
        service.beginMigrateKeeperContainers(models);
        Assert.assertEquals(6, service.getMigrationProcess().get(0).getMigrateKeeperCompleteCount());
        Assert.assertEquals(6, service.getMigrationProcess().get(0).getFinishedShards().size());
    }

    @Test
    public void testClone() {
        KeeperContainerUsedInfoModel target = new KeeperContainerUsedInfoModel()
                .setKeeperIp("2.2.2.2").setDcName("jq").setTotalInputFlow(300 * 1024 * 1024L)
                .setTotalRedisUsedMemory(500 * 1024 * 1024 * 1024L);
        Map<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardKeeper("jq", "cluster1", "shard1", true), new KeeperContainerUsedInfoModel.KeeperUsedInfo(200 * 1024 * 1024L, 400 * 1024 * 1024L, ""));
        target.setDetailInfo(detailInfo2);
        KeeperContainerUsedInfoModel usedInfoModel = KeeperContainerUsedInfoModel.cloneKeeperContainerUsedInfoModel(target);
        for (Map.Entry<DcClusterShardKeeper, KeeperContainerUsedInfoModel.KeeperUsedInfo> entry : detailInfo2.entrySet()) {
            KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel(target, entry);
            Assert.assertEquals(model.getKeeperIp(), usedInfoModel.getKeeperIp());
        }
        Assert.assertEquals(target.getDcName(), usedInfoModel.getDcName());
    }
}