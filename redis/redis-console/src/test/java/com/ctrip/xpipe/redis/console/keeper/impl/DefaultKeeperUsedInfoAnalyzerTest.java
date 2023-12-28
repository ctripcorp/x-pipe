package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yu
 * <p>
 * 2023/9/20
 */
@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class DefaultKeeperUsedInfoAnalyzerTest {

    @InjectMocks
    private DefaultKeeperContainerUsedInfoAnalyzer analyzer;
    @Mock
    private ConsoleConfig config;
    @Mock
    private ThreadPoolExecutor executor;
    @Mock
    private KeeperContainerService keeperContainerService;
    @Mock
    private FoundationService service;
    private final KeeperContainerFilterChain filterChain = new KeeperContainerFilterChain();
    public static final int expireTime = 1000;
    public static final String DC = "jq";
    public static final String IP1 = "1.1.1.1", IP2 = "2.2.2.2", IP3 = "3.3.3.3", IP4 = "4.4.4.4", IP5 = "5.5.5.5";
    public static final String Cluster1 = "cluster1", Cluster2 = "cluster2", Cluster3 = "cluster3", Cluster4 = "cluster4", Cluster5 = "cluster5";
    public static final String Shard1 = "shard1", Shard2 = "shard2", Shard3 = "shard3";

    @Before
    public void before() {
        analyzer.setExecutors(executor);
        analyzer.setKeeperContainerService(keeperContainerService);
        //Disabling activeKeeper/backupKeeper Switch
        filterChain.setConfig(new DefaultConsoleConfig());
        analyzer.setKeeperContainerFilterChain(filterChain);
        Mockito.when(config.getClusterDividedParts()).thenReturn(2);
        List<KeeperContainerOverloadStandardModel.DiskTypesEnum> diskTypeEnums = new ArrayList<>();
        diskTypeEnums.add(new KeeperContainerOverloadStandardModel.DiskTypesEnum(KeeperContainerOverloadStandardModel.DiskType.RAID0, 30, 20));
        Map<String, KeeperContainerOverloadStandardModel> standards = Maps.newHashMap();
        standards.put(FoundationService.DEFAULT.getDataCenter(), new KeeperContainerOverloadStandardModel().setFlowOverload(20).setPeerDataOverload(20).setDiskTypes(diskTypeEnums));
        Mockito.when(config.getKeeperContainerOverloadStandards()).thenReturn(standards);
        Mockito.when(config.getKeeperCheckerIntervalMilli()).thenReturn(expireTime);
        Mockito.when(config.getKeeperContainerOverloadFactor()).thenReturn(0.8);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(5.0);
        KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl();
        keepercontainerTbl.setKeepercontainerActive(true);
        Mockito.when(keeperContainerService.find(Mockito.any())).thenReturn(keepercontainerTbl);
        Mockito.doNothing().when(executor).execute(Mockito.any());
    }

    public KeeperContainerUsedInfoModel createKeeperContainer(Map<String, KeeperContainerUsedInfoModel> models, String keeperIp, long activeInputFlow, long activeRedisUsedMemory){
        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel(keeperIp, DC, activeInputFlow, activeRedisUsedMemory);
        model.setDiskAvailable(true).setDiskUsed(70).setDiskSize(100);
        models.put(keeperIp, model);
        return model;
    }

    @Test
    public void testAnalyzeOverloadStandardModel() {
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        KeeperContainerUsedInfoModel keeper = createKeeperContainer(models, IP1, 14, 14)
                .createKeeper(Cluster1, Shard1, false, 2, 2)
                .createKeeper(Cluster1, Shard2, false, 3, 3);
        KeeperContainerUsedInfoModel keeper1 = createKeeperContainer(models, IP2, 13, 13)
                .createKeeper(Cluster1, Shard1, false, 3, 3)
                .createKeeper(Cluster1, Shard2, false, 4, 4);
        Mockito.when(keeperContainerService.find(Mockito.any())).thenReturn(new KeepercontainerTbl().setKeepercontainerDiskType(KeeperContainerOverloadStandardModel.DiskType.RAID0.getDesc()));
        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerStandard();
        Assert.assertEquals(16, keeper.getInputFlowStandard());
        Assert.assertEquals(16, keeper1.getInputFlowStandard());
        Assert.assertEquals(24, keeper.getRedisUsedMemoryStandard());
        Assert.assertEquals(24, keeper1.getRedisUsedMemoryStandard());
    }

    @Test
    public void testUpdateKeeperContainerUsedInfo() {
        //To prevent a second updateKeeperContainerUsedInfo() data when expired
        Mockito.when(config.getKeeperCheckerIntervalMilli()).thenReturn(1000000);
        Map<String, KeeperContainerUsedInfoModel> models1 = new HashMap<>();
        createKeeperContainer(models1, IP1, 14, 14)
                .createKeeper(Cluster1, Shard1, true, 2, 2)
                .createKeeper(Cluster1, Shard2, true, 3, 3)
                .createKeeper(Cluster2, Shard1, true, 4, 4)
                .createKeeper(Cluster2, Shard1, true, 5, 5);
        analyzer.updateKeeperContainerUsedInfo(0, new ArrayList<>(models1.values()));
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());

        Map<String, KeeperContainerUsedInfoModel> models2 = new HashMap<>();
        createKeeperContainer(models2, IP3, 5, 5)
                .createKeeper(Cluster3, Shard1, true, 2, 2)
                .createKeeper(Cluster4, Shard2, true, 3, 3);
        analyzer.updateKeeperContainerUsedInfo(1, new ArrayList<>(models2.values()));
        Assert.assertEquals(0, analyzer.getCheckerIndexes().size());
        Assert.assertEquals(0, analyzer.getKeeperContainerUsedInfoModelIndexMap().size());
    }

    @Test
    public void testUpdateKeeperContainerUsedInfoExpired() throws InterruptedException {
        Map<String, KeeperContainerUsedInfoModel> models1 = new HashMap<>();
        createKeeperContainer(models1, IP1, 14, 14)
                .createKeeper(Cluster1, Shard1, true, 2, 2)
                .createKeeper(Cluster1, Shard2, true, 3, 3)
                .createKeeper(Cluster2, Shard1, true, 4, 4)
                .createKeeper(Cluster2, Shard1, true, 5, 5);
        analyzer.updateKeeperContainerUsedInfo(0, new ArrayList<>(models1.values()));
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());

        TimeUnit.MILLISECONDS.sleep(expireTime+100);

        Map<String, KeeperContainerUsedInfoModel> models2 = new HashMap<>();
        createKeeperContainer(models2, IP3, 5, 5)
                .createKeeper(Cluster3, Shard1, true, 2, 2)
                .createKeeper(Cluster4, Shard2, true, 3, 3);
        analyzer.updateKeeperContainerUsedInfo(0, new ArrayList<>(models2.values()));
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());
        Assert.assertEquals(1, analyzer.getKeeperContainerUsedInfoModelIndexMap().size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithBoth() {
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 10, 10)
                .createKeeper(Cluster1, Shard1, true, 4, 4)
                .createKeeper(Cluster1, Shard2, false, 6, 6)
                .createKeeper(Cluster2, Shard1, false, 6, 6)
                .createKeeper(Cluster2, Shard2, true, 6, 6)
                .createKeeper(Cluster3, Shard1, false, 4, 4);

        createKeeperContainer(models, IP2, 20, 20)
                .createKeeper(Cluster1, Shard2, true, 6, 6)
                .createKeeper(Cluster3, Shard1, false, 4, 4)
                .createKeeper(Cluster3, Shard2, true, 6, 6)
                .createKeeper(Cluster4, Shard1, true, 8, 8)
                .createKeeper(Cluster4, Shard2, false, 6, 6);

        createKeeperContainer(models, IP3, 20, 20)
                .createKeeper(Cluster2, Shard1, true, 6, 6)
                .createKeeper(Cluster3, Shard2, false, 6, 6)
                .createKeeper(Cluster3, Shard1, true, 6, 6)
                .createKeeper(Cluster4, Shard1, false, 8, 8)
                .createKeeper(Cluster4, Shard2, true, 6, 6);

        createKeeperContainer(models, IP4, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 4, 4)
                .createKeeper(Cluster2, Shard2, false, 6, 6);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals(IP2, allDcReadyToMigrationKeeperContainers.get(0).getSrcKeeperContainer().getKeeperIp());
        Assert.assertEquals(IP4, allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(IP3, allDcReadyToMigrationKeeperContainers.get(1).getSrcKeeperContainer().getKeeperIp());
        Assert.assertEquals(IP4, allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithBoth() {
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 22, 22)
                .createKeeper(Cluster1, Shard1, true, 4, 4)
                .createKeeper(Cluster1, Shard2, true, 5, 5)
                .createKeeper(Cluster2, Shard1, true, 6, 6)
                .createKeeper(Cluster2, Shard2, true, 7, 7);

        createKeeperContainer(models, IP2, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 4, 4)
                .createKeeper(Cluster1, Shard2, false, 5, 5)
                .createKeeper(Cluster2, Shard1, false, 6, 6)
                .createKeeper(Cluster2, Shard2, false, 7, 7);

        createKeeperContainer(models, IP3, 22, 22)
                .createKeeper(Cluster3, Shard1, true, 4, 4)
                .createKeeper(Cluster3, Shard2, true, 5, 5)
                .createKeeper(Cluster4, Shard1, true, 6, 6)
                .createKeeper(Cluster4, Shard2, true, 7, 7);

        createKeeperContainer(models, IP4, 0, 0)
                .createKeeper(Cluster3, Shard1, false, 4, 4)
                .createKeeper(Cluster3, Shard2, false, 5, 5)
                .createKeeper(Cluster4, Shard1, false, 6, 6)
                .createKeeper(Cluster4, Shard2, false, 7, 7);

        createKeeperContainer(models, IP5, 0, 0);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals(IP5, allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(IP5, allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());

    }

    @Test
    public void testSingleSrcKeeperMultiTargetWithBoth() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 27, 27)
                .createKeeper(Cluster1, Shard1, true, 5, 5)
                .createKeeper(Cluster1, Shard2, true, 5, 5)
                .createKeeper(Cluster2, Shard1, true, 3, 3)
                .createKeeper(Cluster2, Shard2, true, 4, 4)
                .createKeeper(Cluster3, Shard1, true, 5, 5)
                .createKeeper(Cluster3, Shard2, true, 5, 5);

        createKeeperContainer(models, IP2, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 5, 5)
                .createKeeper(Cluster1, Shard2, false, 5, 5)
                .createKeeper(Cluster2, Shard1, false, 3, 3)
                .createKeeper(Cluster2, Shard2, false, 4, 4)
                .createKeeper(Cluster3, Shard1, false, 5, 5)
                .createKeeper(Cluster3, Shard2, false, 5, 5);

        createKeeperContainer(models, IP3, 5, 5)
                .createKeeper(Cluster4, Shard1, true, 5, 5)
                .createKeeper(Cluster4, Shard2, false, 10, 10);

        createKeeperContainer(models, IP4, 10, 10)
                .createKeeper(Cluster4, Shard1, false, 5, 5)
                .createKeeper(Cluster4, Shard2, true, 10, 10);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals(IP3, allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals(IP4, allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }

    @Test
    public void testKeeperResourceLackWithBoth() {
        Map<String, KeeperContainerUsedInfoModel> models1 = new HashMap<>();
        createKeeperContainer(models1, IP1, 20, 20)
                .createKeeper(Cluster1, Shard1, true, 10, 10)
                .createKeeper(Cluster1, Shard2, true, 10, 10)
                .createKeeper(Cluster2, Shard1, false, 10, 10)
                .createKeeper(Cluster2, Shard2, false, 10, 10);

        createKeeperContainer(models1, IP2, 20, 20)
                .createKeeper(Cluster2, Shard1, true, 10, 10)
                .createKeeper(Cluster2, Shard2, true, 10, 10)
                .createKeeper(Cluster1, Shard1, false, 10, 10)
                .createKeeper(Cluster1, Shard2, false, 10, 10);;

        createKeeperContainer(models1, IP3, 0, 0);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithPeerDataOverLoad() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 4, 23)
                .createKeeper(Cluster1, Shard1, true, 1, 6)
                .createKeeper(Cluster1, Shard2, true, 1, 5)
                .createKeeper(Cluster2, Shard1, true, 1, 6)
                .createKeeper(Cluster2, Shard2, true, 1, 6)
                .createKeeper(Cluster3, Shard1, false, 1, 6)
                .createKeeper(Cluster3, Shard2, false, 1, 5)
                .createKeeper(Cluster4, Shard1, false, 1, 6)
                .createKeeper(Cluster4, Shard2, false, 1, 6);

        createKeeperContainer(models, IP2, 4, 23)
                .createKeeper(Cluster3, Shard1, true, 1, 6)
                .createKeeper(Cluster3, Shard2, true, 1, 5)
                .createKeeper(Cluster4, Shard1, true, 1, 6)
                .createKeeper(Cluster4, Shard2, true, 1, 6)
                .createKeeper(Cluster1, Shard1, false, 1, 6)
                .createKeeper(Cluster1, Shard2, false, 1, 5)
                .createKeeper(Cluster2, Shard1, false, 1, 6)
                .createKeeper(Cluster2, Shard2, false, 1, 6);

        createKeeperContainer(models, IP3, 1, 2)
                .createKeeper(Cluster5, Shard1, true, 1, 2)
                .createKeeper(Cluster5, Shard2, false, 1, 2);

        createKeeperContainer(models, IP4, 1, 2)
                .createKeeper(Cluster5, Shard2, true, 1, 2)
                .createKeeper(Cluster5, Shard1, false, 1, 2);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.stream().filter(container -> !container.isKeeperPairOverload()).count());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithPeerDataOverLoad() {
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 4, 17)
                .createKeeper(Cluster1, Shard1, true, 1, 5)
                .createKeeper(Cluster1, Shard2, true, 1, 5)
                .createKeeper(Cluster2, Shard1, true, 1, 5)
                .createKeeper(Cluster2, Shard2, true, 1, 2)
                .createKeeper(Cluster3, Shard1, false, 1, 5)
                .createKeeper(Cluster3, Shard2, false, 1, 5)
                .createKeeper(Cluster4, Shard1, false, 1, 5)
                .createKeeper(Cluster4, Shard2, false, 1, 2);

        createKeeperContainer(models, IP2, 4, 17)
                .createKeeper(Cluster3, Shard1, true, 1, 5)
                .createKeeper(Cluster3, Shard2, true, 1, 5)
                .createKeeper(Cluster4, Shard1, true, 1, 5)
                .createKeeper(Cluster4, Shard2, true, 1, 2)
                .createKeeper(Cluster1, Shard1, false, 1, 5)
                .createKeeper(Cluster1, Shard2, false, 1, 5)
                .createKeeper(Cluster2, Shard1, false, 1, 5)
                .createKeeper(Cluster2, Shard2, false, 1, 2);

        createKeeperContainer(models, IP3, 0, 0);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.stream().filter(container -> !container.isKeeperPairOverload()).count());
    }

    @Test
    public void testSingleSrcKeeperMultiTargetWithPeerDataOverLoad() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 6, 32)
                .createKeeper(Cluster1, Shard1, true, 1, 5)
                .createKeeper(Cluster1, Shard2, true, 1, 5)
                .createKeeper(Cluster2, Shard1, true, 1, 5)
                .createKeeper(Cluster2, Shard2, true, 1, 5)
                .createKeeper(Cluster3, Shard1, true, 1, 6)
                .createKeeper(Cluster3, Shard2, true, 1, 6);

        createKeeperContainer(models, IP2, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 1, 5)
                .createKeeper(Cluster1, Shard2, false, 1, 5)
                .createKeeper(Cluster2, Shard1, false, 1, 5)
                .createKeeper(Cluster2, Shard2, false, 1, 5)
                .createKeeper(Cluster3, Shard1, false, 1, 6)
                .createKeeper(Cluster3, Shard2, false, 1, 6);

        createKeeperContainer(models, IP3,0,0);
        createKeeperContainer(models, IP4,0,0);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }

    @Test
    public void testKeeperResourceLackWithPeerDataOverLoad() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 6, 32)
                .createKeeper(Cluster1, Shard1, true, 1, 5)
                .createKeeper(Cluster1, Shard2, true, 1, 5)
                .createKeeper(Cluster2, Shard1, true, 1, 5)
                .createKeeper(Cluster2, Shard2, true, 1, 5)
                .createKeeper(Cluster3, Shard1, true, 1, 6)
                .createKeeper(Cluster3, Shard2, true, 1, 6);

        createKeeperContainer(models, IP2, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 1, 5)
                .createKeeper(Cluster1, Shard2, false, 1, 5)
                .createKeeper(Cluster2, Shard1, false, 1, 5)
                .createKeeper(Cluster2, Shard2, false, 1, 5)
                .createKeeper(Cluster3, Shard1, false, 1, 6)
                .createKeeper(Cluster3, Shard2, false, 1, 6);

        createKeeperContainer(models, IP3,0,0);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithMixed() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        // inputOverLoad
        createKeeperContainer(models, IP1, 20, 4)
                .createKeeper(Cluster1, Shard1, true, 4, 1)
                .createKeeper(Cluster1, Shard2, true, 4, 1)
                .createKeeper(Cluster2, Shard1, true, 6, 1)
                .createKeeper(Cluster2, Shard2, true, 6, 1)
                .createKeeper(Cluster3, Shard1, false, 1, 4)
                .createKeeper(Cluster3, Shard2, false, 1, 4)
                .createKeeper(Cluster4, Shard1, false, 1, 6)
                .createKeeper(Cluster4, Shard2, false, 1, 6);

        //PeerDataOverLoad
        createKeeperContainer(models, IP2, 4, 20)
                .createKeeper(Cluster3, Shard1, true, 1, 4)
                .createKeeper(Cluster3, Shard2, true, 1, 4)
                .createKeeper(Cluster4, Shard1, true, 1, 6)
                .createKeeper(Cluster4, Shard2, true, 1, 6)
                .createKeeper(Cluster1, Shard1, false, 4, 1)
                .createKeeper(Cluster1, Shard2, false, 4, 1)
                .createKeeper(Cluster2, Shard1, false, 6, 1)
                .createKeeper(Cluster2, Shard2, false, 6, 1);

        createKeeperContainer(models, IP3, 9, 9)
                .createKeeper(Cluster5, Shard1, true, 9, 9)
                .createKeeper(Cluster5, Shard2, false, 9, 9);

        createKeeperContainer(models, IP3, 9, 9)
                .createKeeper(Cluster5, Shard2, true, 9, 9)
                .createKeeper(Cluster5, Shard1, false, 9, 9);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.stream().filter(container -> !container.isKeeperPairOverload()).count());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithMixed() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 4, 19)
                .createKeeper(Cluster1, Shard1, true, 1, 8)
                .createKeeper(Cluster1, Shard2, true, 1, 7)
                .createKeeper(Cluster2, Shard1, true, 1, 2)
                .createKeeper(Cluster2, Shard2, true, 1, 2)
                .createKeeper(Cluster3, Shard1, false, 1, 8)
                .createKeeper(Cluster3, Shard2, false, 1, 7)
                .createKeeper(Cluster4, Shard1, false, 1, 2)
                .createKeeper(Cluster4, Shard2, false, 1, 2);

        createKeeperContainer(models, IP2, 4, 19)
                .createKeeper(Cluster3, Shard1, true, 1, 8)
                .createKeeper(Cluster3, Shard2, true, 1, 7)
                .createKeeper(Cluster4, Shard1, true, 1, 2)
                .createKeeper(Cluster4, Shard2, true, 1, 2)
                .createKeeper(Cluster1, Shard1, false, 1, 8)
                .createKeeper(Cluster1, Shard2, false, 1, 7)
                .createKeeper(Cluster2, Shard1, false, 1, 2)
                .createKeeper(Cluster2, Shard2, false, 1, 2);

        createKeeperContainer(models, IP3, 0, 0);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.stream().filter(container -> !container.isKeeperPairOverload()).count());
    }

    @Test
    public void testMultiSrcMultiTargetWithFixed() {
        filterChain.setConfig(config);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 4, 19)
                .createKeeper(Cluster1, Shard1, true, 1, 8)
                .createKeeper(Cluster1, Shard2, true, 1, 7)
                .createKeeper(Cluster2, Shard1, true, 1, 2)
                .createKeeper(Cluster2, Shard2, true, 1, 2)
                .createKeeper(Cluster3, Shard1, false, 1, 8)
                .createKeeper(Cluster3, Shard2, false, 1, 7)
                .createKeeper(Cluster4, Shard1, false, 1, 2)
                .createKeeper(Cluster4, Shard2, false, 1, 2);

        createKeeperContainer(models, IP2, 4, 19)
                .createKeeper(Cluster3, Shard1, true, 1, 8)
                .createKeeper(Cluster3, Shard2, true, 1, 7)
                .createKeeper(Cluster4, Shard1, true, 1, 2)
                .createKeeper(Cluster4, Shard2, true, 1, 2)
                .createKeeper(Cluster1, Shard1, false, 1, 8)
                .createKeeper(Cluster1, Shard2, false, 1, 7)
                .createKeeper(Cluster2, Shard1, false, 1, 2)
                .createKeeper(Cluster2, Shard2, false, 1, 2);

        createKeeperContainer(models, IP3, 8, 1)
                .createKeeper(Cluster5, Shard1, true, 8, 1)
                .createKeeper(Cluster5, Shard2, false, 8, 1);

        createKeeperContainer(models, IP4, 1, 8)
                .createKeeper(Cluster5, Shard2, true, 1, 8)
                .createKeeper(Cluster5, Shard1, false, 8, 1);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.stream().filter(container -> !container.isKeeperPairOverload()).count());
    }

    @Test
    public void testSwitchActiveKeeper() {
        filterChain.setConfig(config);
        Map<String, KeeperContainerOverloadStandardModel> standards = Maps.newHashMap();
        standards.put(FoundationService.DEFAULT.getDataCenter(), new KeeperContainerOverloadStandardModel().setFlowOverload(100).setPeerDataOverload(100));
        Mockito.when(config.getKeeperContainerOverloadStandards()).thenReturn(standards);
        Map<String, KeeperContainerUsedInfoModel> models = new HashMap<>();
        createKeeperContainer(models, IP1, 81, 81)
                .createKeeper(Cluster1, Shard1, true, 15, 15)
                .createKeeper(Cluster1, Shard2, true, 15, 15)
                .createKeeper(Cluster1, Shard3, true, 15, 15)
                .createKeeper(Cluster2, Shard1, true, 16, 16)
                .createKeeper(Cluster2, Shard2, true, 10, 10)
                .createKeeper(Cluster2, Shard3, true, 10, 10);

        createKeeperContainer(models, IP2, 0, 0)
                .createKeeper(Cluster2, Shard1, false, 16, 16);

        createKeeperContainer(models, IP3, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 15, 15)
                .createKeeper(Cluster1, Shard2, false, 15, 15)
                .createKeeper(Cluster1, Shard3, false, 15, 15)
                .createKeeper(Cluster2, Shard2, false, 10, 10)
                .createKeeper(Cluster2, Shard3, false, 10, 10);

        analyzer.getCurrentDcKeeperContainerUsedInfoModelsList().putAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getCurrentDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals(true,  allDcReadyToMigrationKeeperContainers.get(0).isSwitchActive());

    }
}