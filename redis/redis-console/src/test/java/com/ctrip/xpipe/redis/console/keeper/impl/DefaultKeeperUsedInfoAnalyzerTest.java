package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardActive;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.keeper.handler.KeeperContainerFilterChain;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.impl.KeeperContainerServiceImpl;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
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
    public static final String IP1 = "1.1.1.1", IP2 = "2.2.2.2", IP3 = "3.3.3.3", IP4 = "4.4.4.4";
    public static final String Cluster1 = "cluster1", Cluster2 = "cluster2", Cluster3 = "cluster3", Cluster4 = "cluster4";
    public static final String Shard1 = "shard1", Shard2 = "shard2", Shard3 = "shard3", Shard4 = "shard4";

    @Before
    public void before() {
        analyzer.setExecutors(executor);
        analyzer.setKeeperContainerService(keeperContainerService);
        filterChain.setConfig(new DefaultConsoleConfig());
        analyzer.setKeeperContainerFilterChain(filterChain);
        Mockito.when(config.getClusterDividedParts()).thenReturn(2);
        Map<String, KeeperContainerOverloadStandardModel> standards = Maps.newHashMap();
        List<KeeperContainerOverloadStandardModel.DiskTypesEnum> diskTypeEnums = new ArrayList<>();
        diskTypeEnums.add(new KeeperContainerOverloadStandardModel.DiskTypesEnum(KeeperContainerOverloadStandardModel.DiskType.RAID0, 30, 20));
        standards.put(FoundationService.DEFAULT.getDataCenter(), new KeeperContainerOverloadStandardModel().setFlowOverload(10).setPeerDataOverload(10).setDiskTypes(diskTypeEnums));
        Mockito.when(config.getKeeperContainerOverloadStandards()).thenReturn(standards);
        Mockito.when(config.getKeeperCheckerIntervalMilli()).thenReturn(expireTime);
        Mockito.when(config.getKeeperContainerOverloadFactor()).thenReturn(0.8);
        Mockito.when(config.getKeeperPairOverLoadFactor()).thenReturn(1.0);
        KeepercontainerTbl keepercontainerTbl = new KeepercontainerTbl();
        keepercontainerTbl.setKeepercontainerActive(true);
        Mockito.when(keeperContainerService.find(Mockito.any())).thenReturn(keepercontainerTbl);
        Mockito.doNothing().when(executor).execute(Mockito.any());
    }

    public KeeperContainerUsedInfoModel createKeeperContainer(List<KeeperContainerUsedInfoModel> models, String keeperIp, long activeInputFlow, long activeRedisUsedMemory){
        KeeperContainerUsedInfoModel model = new KeeperContainerUsedInfoModel(keeperIp, DC, activeInputFlow, activeRedisUsedMemory);
        model.setDiskAvailable(true).setDiskUsed(70).setDiskSize(100);
        models.add(model);
        return model;
    }

    @Test
    public void testAnalyzeOverloadStandardModel() {
        List<KeeperContainerUsedInfoModel> models = new ArrayList<>();
        createKeeperContainer(models, IP1, 14, 14)
                .createKeeper(Cluster1, Shard1, false, 2, 2)
                .createKeeper(Cluster1, Shard2, false, 3, 3);
        createKeeperContainer(models, IP2, 13, 13)
                .createKeeper(Cluster1, Shard1, false, 3, 3)
                .createKeeper(Cluster1, Shard2, false, 4, 4);
        Mockito.when(keeperContainerService.find(Mockito.any())).thenReturn(new KeepercontainerTbl().setKeepercontainerDiskType(KeeperContainerOverloadStandardModel.DiskType.RAID0.getDesc()));
        analyzer.updateKeeperContainerUsedInfo(0, models);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models);
        Map<String, KeeperContainerOverloadStandardModel> overloadStandardModelMap = analyzer.analyzeKeeperContainerStandard();
        Assert.assertEquals(16, overloadStandardModelMap.get(IP1).getFlowOverload());
        Assert.assertEquals(16, overloadStandardModelMap.get(IP2).getFlowOverload());
        Assert.assertEquals(24, overloadStandardModelMap.get(IP1).getPeerDataOverload());
        Assert.assertEquals(24, overloadStandardModelMap.get(IP2).getPeerDataOverload());
    }

    @Test
    public void testUpdateKeeperContainerUsedInfo() {
        //To prevent a second updateKeeperContainerUsedInfo() data when expired
        Mockito.when(config.getKeeperCheckerIntervalMilli()).thenReturn(1000000);
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        createKeeperContainer(models1, IP1, 14, 14)
                .createKeeper(Cluster1, Shard1, true, 2, 2)
                .createKeeper(Cluster1, Shard2, true, 3, 3)
                .createKeeper(Cluster2, Shard1, true, 4, 4)
                .createKeeper(Cluster2, Shard1, true, 5, 5);
        analyzer.updateKeeperContainerUsedInfo(0, models1);
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        createKeeperContainer(models2, IP3, 5, 5)
                .createKeeper(Cluster3, Shard1, true, 2, 2)
                .createKeeper(Cluster4, Shard2, true, 3, 3);
        analyzer.updateKeeperContainerUsedInfo(1, models2);
        Assert.assertEquals(0, analyzer.getCheckerIndexes().size());
        Assert.assertEquals(0, analyzer.getAllKeeperContainerUsedInfoModels().size());
    }

    @Test
    public void testUpdateKeeperContainerUsedInfoExpired() throws InterruptedException {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        createKeeperContainer(models1, IP1, 14, 14)
                .createKeeper(Cluster1, Shard1, true, 2, 2)
                .createKeeper(Cluster1, Shard2, true, 3, 3)
                .createKeeper(Cluster2, Shard1, true, 4, 4)
                .createKeeper(Cluster2, Shard1, true, 5, 5);
        analyzer.updateKeeperContainerUsedInfo(0, models1);
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());

        TimeUnit.MILLISECONDS.sleep(expireTime+100);

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        createKeeperContainer(models2, IP3, 5, 5)
                .createKeeper(Cluster3, Shard1, true, 2, 2)
                .createKeeper(Cluster4, Shard2, true, 3, 3);
        analyzer.updateKeeperContainerUsedInfo(1, models2);
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());
        Assert.assertEquals(1, analyzer.getAllKeeperContainerUsedInfoModels().size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithBoth() {
        List<KeeperContainerUsedInfoModel> models = new ArrayList<>();
        createKeeperContainer(models, IP1, 5, 5)
                .createKeeper(Cluster1, Shard1, true, 2, 2)
                .createKeeper(Cluster1, Shard2, false, 3, 3)
                .createKeeper(Cluster2, Shard1, false, 4, 4)
                .createKeeper(Cluster2, Shard2, true, 3, 3)
                .createKeeper(Cluster3, Shard1, false, 2, 2);

        createKeeperContainer(models, IP2, 10, 10)
                .createKeeper(Cluster1, Shard2, true, 3, 3)
                .createKeeper(Cluster3, Shard1, false, 2, 2)
                .createKeeper(Cluster3, Shard2, true, 3, 3)
                .createKeeper(Cluster4, Shard1, true, 4, 4)
                .createKeeper(Cluster4, Shard2, false, 3, 3);

        createKeeperContainer(models, IP3, 10, 10)
                .createKeeper(Cluster2, Shard1, true, 4, 4)
                .createKeeper(Cluster3, Shard2, false, 3, 3)
                .createKeeper(Cluster3, Shard1, true, 3, 3)
                .createKeeper(Cluster4, Shard1, false, 4, 4)
                .createKeeper(Cluster4, Shard2, true, 3, 3);

        createKeeperContainer(models, IP4, 0, 0)
                .createKeeper(Cluster1, Shard1, false, 2, 2)
                .createKeeper(Cluster2, Shard2, false, 3, 3);

        analyzer.updateKeeperContainerUsedInfo(0, models);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithBoth() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(4L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(5L, 5L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(4L, 4L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(5L, 5L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 1, 1);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());

    }

    @Test
    public void testSingleSrcKeeperMultiTargetWithBoth() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 17, 17);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 5, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(5L, 5L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 4, 4);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(4L, 4L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals("2.2.2.2", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }

    @Test
    public void testKeeperResourceLackWithBoth() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(4L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(5L, 5L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(4L, 4L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo3.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 4, 14);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 5L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 4, 13);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 2, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 2, 6);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo4 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 4, 14);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 5L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 4, 13);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        detailInfo2.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 1, 1);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 1L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());

    }

    @Test
    public void testSingleSrcKeeperMultiTargetWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 6, 17);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 1, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 5L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 1, 4);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals("2.2.2.2", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }

    @Test
    public void testKeeperResourceLackWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 4, 14);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 5L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 4, 13);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(1L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(1L, 5L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 2, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(1L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithMixed() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        // inputOverLoad
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 8);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(3L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(4L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(5L, 2L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        //PeerDataOverLoad
        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 8, 13);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(2L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(2L, 4L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(2L, 3L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(2L, 2L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo4 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(3L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(3L, 3L, ""));
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithMixed() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 5, 15);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard3", true), new KeeperUsedInfo(1L, 3L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 15, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster5", "shard2", true), new KeeperUsedInfo(3L, 1L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 0, 0);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }

    @Test
    public void testMultiSrcMultiTargetWithFixed() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 5, 15);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster1", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard1", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard2", true), new KeeperUsedInfo(1L, 3L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster2", "shard3", true), new KeeperUsedInfo(1L, 3L, ""));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 15, 5);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo2 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard1", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster3", "shard2", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard1", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster4", "shard2", true), new KeeperUsedInfo(3L, 1L, ""));
        detailInfo1.put(new DcClusterShardActive("jq", "cluster5", "shard2", true), new KeeperUsedInfo(3L, 1L, ""));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 6, 4);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo3 = Maps.newHashMap();
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 4, 6);
        Map<DcClusterShardActive, KeeperUsedInfo> detailInfo4 = Maps.newHashMap();
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.getAllKeeperContainerUsedInfoModelsList().addAll(models1);
        analyzer.analyzeKeeperContainerUsedInfo();

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals("4.4.4.4", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }
}