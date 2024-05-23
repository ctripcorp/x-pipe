package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yu
 * <p>
 * 2023/9/27
 */

@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class AutoMigrateOverloadKeeperContainerActionTest {

    @InjectMocks
    AutoMigrateOverloadKeeperContainerAction action;

    @Mock
    private ShardModelService shardModelService;

    @Mock
    private AlertManager alertManager;

    @Before
    public void beforeAutoMigrateOverloadKeeperContainerActionTest() {
        ShardModel shardModel = new ShardModel();
        Mockito.when(shardModelService.getShardModel(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),  Mockito.anyObject()))
                .thenReturn(shardModel);
        Mockito.when(shardModelService.migrateBackupKeeper(Mockito.anyString(), Mockito.anyString(),  Mockito.any(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(true);
    }

    @Test
    public void testMigrateAllKeepersSuccess() {
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);

        List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers = new ArrayList<>();
        List<DcClusterShard> migrationShards1 = new ArrayList<>();
        migrationShards1.add(new DcClusterShard("jq", "cluster2", "shard2"));
        migrationShards1.add(new DcClusterShard("jq", "cluster4", "shard2"));
        migrationShards1.add(new DcClusterShard("jq", "cluster5", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel1 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model1).setTargetKeeperContainer(model3).setMigrateKeeperCount(3).setMigrateShards(migrationShards1);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel1);

        List<DcClusterShard> migrationShards2 = new ArrayList<>();
        migrationShards2.add(new DcClusterShard("jq", "cluster14", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster15", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster16", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster17", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel2 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model2).setTargetKeeperContainer(model4).setMigrateKeeperCount(4).setMigrateShards(migrationShards2);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel2);

        action.migrateAllKeepers(readyToMigrationKeeperContainers);

        Assert.assertEquals(3, migrationKeeperContainerDetailModel1.getMigrateKeeperCompleteCount());
        Assert.assertEquals(4, migrationKeeperContainerDetailModel2.getMigrateKeeperCompleteCount());
    }

    @Test
    public void testMigrateAllKeepersFail() {
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);

        List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers = new ArrayList<>();
        List<DcClusterShard> migrationShards1 = new ArrayList<>();
        migrationShards1.add(new DcClusterShard("jq", "cluster2", "shard2"));
        migrationShards1.add(new DcClusterShard("jq", "cluster4", "shard2"));
        migrationShards1.add(new DcClusterShard("jq", "cluster5", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel1 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model1).setTargetKeeperContainer(model3).setMigrateKeeperCount(3).setMigrateShards(migrationShards1);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel1);

        List<DcClusterShard> migrationShards2 = new ArrayList<>();
        migrationShards2.add(new DcClusterShard("jq", "cluster14", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster15", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster16", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster17", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel2 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model2).setTargetKeeperContainer(model4).setMigrateKeeperCount(4).setMigrateShards(migrationShards2);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel2);

        Mockito.when(shardModelService.migrateBackupKeeper(Mockito.anyString(), Mockito.anyString(),  Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        action.migrateAllKeepers(readyToMigrationKeeperContainers);

        Assert.assertEquals(0, migrationKeeperContainerDetailModel1.getMigrateKeeperCompleteCount());
        Assert.assertEquals(0, migrationKeeperContainerDetailModel2.getMigrateKeeperCompleteCount());
    }

    @Test
    public void testMigrateAllKeepersWithSameShard() {
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);

        List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers = new ArrayList<>();
        List<DcClusterShard> migrationShards1 = new ArrayList<>();
        migrationShards1.add(new DcClusterShard("jq", "cluster2", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel1 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model1).setTargetKeeperContainer(model3).setMigrateKeeperCount(1).setMigrateShards(migrationShards1);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel1);

        List<DcClusterShard> migrationShards2 = new ArrayList<>();
        migrationShards2.add(new DcClusterShard("jq", "cluster2", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel2 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model2).setTargetKeeperContainer(model4).setMigrateKeeperCount(1).setMigrateShards(migrationShards2);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel2);

        action.migrateAllKeepers(readyToMigrationKeeperContainers);

        Assert.assertEquals(1, migrationKeeperContainerDetailModel1.getMigrateKeeperCompleteCount());
        Assert.assertEquals(0, migrationKeeperContainerDetailModel2.getMigrateKeeperCompleteCount());
    }

    @Test
    public void testMigrateAllKeepersWithEmptyShard() {
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);

        List<MigrationKeeperContainerDetailModel> readyToMigrationKeeperContainers = new ArrayList<>();
        List<DcClusterShard> migrationShards1 = new ArrayList<>();
        migrationShards1.add(new DcClusterShard("jq", "cluster2", "shard2"));
        migrationShards1.add(new DcClusterShard("jq", "cluster4", "shard2"));
        migrationShards1.add(new DcClusterShard("jq", "cluster5", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel1 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model1).setTargetKeeperContainer(model3).setMigrateKeeperCount(3).setMigrateShards(migrationShards1);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel1);

        List<DcClusterShard> migrationShards2 = new ArrayList<>();
        migrationShards2.add(new DcClusterShard("jq", "cluster4", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster5", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster16", "shard2"));
        migrationShards2.add(new DcClusterShard("jq", "cluster17", "shard2"));
        MigrationKeeperContainerDetailModel migrationKeeperContainerDetailModel2 = new MigrationKeeperContainerDetailModel()
                .setSrcKeeperContainer(model2).setTargetKeeperContainer(model4).setMigrateKeeperCount(4).setMigrateShards(migrationShards2);
        readyToMigrationKeeperContainers.add(migrationKeeperContainerDetailModel2);

        action.migrateAllKeepers(readyToMigrationKeeperContainers);

        Assert.assertEquals(3, migrationKeeperContainerDetailModel1.getMigrateKeeperCompleteCount());
        Assert.assertEquals(2, migrationKeeperContainerDetailModel2.getMigrateKeeperCompleteCount());
    }


}