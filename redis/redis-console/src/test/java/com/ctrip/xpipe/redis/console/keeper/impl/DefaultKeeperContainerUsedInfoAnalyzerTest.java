package com.ctrip.xpipe.redis.console.keeper.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.checker.model.KeeperContainerUsedInfoModel;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.KeeperContainerOverloadStandardModel;
import com.ctrip.xpipe.redis.console.model.MigrationKeeperContainerDetailModel;
import com.ctrip.xpipe.tuple.Pair;
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
public class DefaultKeeperContainerUsedInfoAnalyzerTest {

    @InjectMocks
    private DefaultKeeperContainerUsedInfoAnalyzer analyzer;

    @Mock
    private ConsoleConfig config;

    @Mock
    private ThreadPoolExecutor executor;

    @Mock
    private FoundationService service;


    @Before
    public void before() {
        Mockito.when(config.getClusterDividedParts()).thenReturn(2);
        Map<String, KeeperContainerOverloadStandardModel> standards = Maps.newHashMap();
        standards.put(FoundationService.DEFAULT.getDataCenter(), new KeeperContainerOverloadStandardModel().setFlowOverload(10).setPeerDataOverload(10));
        Mockito.when(config.getKeeperContainerOverloadStandards()).thenReturn(standards);
        Mockito.when(config.getKeeperCheckerIntervalMilli()).thenReturn(10 * 1000);
        Mockito.doNothing().when(executor).execute(Mockito.any());
    }

    @Test
    public void testUpdateKeeperContainerUsedInfo() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(4L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(5L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model3.setDetailInfo(detailInfo3);
        models2.add(model3);

        analyzer.updateKeeperContainerUsedInfo(1, models2);
        Assert.assertEquals(0, analyzer.getCheckerIndexes().size());
        Assert.assertEquals(0, analyzer.getAllKeeperContainerUsedInfoModels().size());
    }

    @Test
    public void testUpdateKeeperContainerUsedInfoExpired() throws InterruptedException {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(4L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(5L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());

        TimeUnit.MILLISECONDS.sleep(11 * 1000);

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model3.setDetailInfo(detailInfo3);
        models2.add(model3);

        analyzer.updateKeeperContainerUsedInfo(1, models2);
        Assert.assertEquals(1, analyzer.getCheckerIndexes().size());
        Assert.assertEquals(1, analyzer.getAllKeeperContainerUsedInfoModels().size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithBoth() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(4L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(5L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(3L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(4L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo4 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(3L, 3L));
        detailInfo3.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(3L, 3L));
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.analyzeKeeperContainerUsedInfo(models1);
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithBoth() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 14);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(4L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(5L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(3L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(4L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 1, 1);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 1L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.analyzeKeeperContainerUsedInfo(models1);

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());

    }

    @Test
    public void testSingleSrcKeeperMultiTargetWithBoth() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 17, 17);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(3L, 3L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(5L, 5L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 4, 4);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(4L, 4L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.analyzeKeeperContainerUsedInfo(models1);

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
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(4L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(5L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 13, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(3L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(4L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.analyzeKeeperContainerUsedInfo(models1);
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 4, 14);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(1L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 4, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(1L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(1L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(1L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 2, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(1L, 3L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 2, 6);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo4 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 3L));
        detailInfo3.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 3L));
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.analyzeKeeperContainerUsedInfo(models1);
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 4, 14);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(1L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 4, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(1L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(1L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(1L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 1, 1);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 1L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.analyzeKeeperContainerUsedInfo(models1);

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());

    }

    @Test
    public void testSingleSrcKeeperMultiTargetWithPeerDataOverLoad() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 6, 17);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(1L, 3L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 1, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 5L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 1, 4);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 4L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.analyzeKeeperContainerUsedInfo(models1);

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
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(1L, 4L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 5L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 4, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(1L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(1L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(1L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 2, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(1L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(1L, 3L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.analyzeKeeperContainerUsedInfo(models1);
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(1, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testGetAllDcReadyToMigrationKeeperContainersWithMixed() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        // inputOverLoad
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 14, 8);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(2L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(3L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(4L, 2L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(5L, 2L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        //PeerDataOverLoad
        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 8, 13);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(2L, 3L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(2L, 4L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(2L, 3L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);


        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo4 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(3L, 3L));
        detailInfo3.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(3L, 3L));
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        analyzer.analyzeKeeperContainerUsedInfo(models1);
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }

    @Test
    public void testMultiSrcKeeperSingleTargetWithMixed() {
        List<KeeperContainerUsedInfoModel> models1 = new ArrayList<>();
        KeeperContainerUsedInfoModel model1 = new KeeperContainerUsedInfoModel("1.1.1.1", "jq", 5, 15);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard3"), new Pair<>(1L, 3L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 15, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster5", "shard2"), new Pair<>(3L, 1L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 0, 0);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        analyzer.analyzeKeeperContainerUsedInfo(models1);

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
        Map<DcClusterShard, Pair<Long, Long>> detailInfo1 = Maps.newHashMap();
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster1", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard1"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(1L, 3L));
        detailInfo1.put(new DcClusterShard("jq", "cluster2", "shard3"), new Pair<>(1L, 3L));
        model1.setDetailInfo(detailInfo1);
        models1.add(model1);

        KeeperContainerUsedInfoModel model2 = new KeeperContainerUsedInfoModel("2.2.2.2", "jq", 15, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo2 = Maps.newHashMap();
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster3", "shard2"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard1"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 1L));
        detailInfo2.put(new DcClusterShard("jq", "cluster5", "shard2"), new Pair<>(3L, 1L));
        model2.setDetailInfo(detailInfo2);
        models1.add(model2);

        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 6, 4);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        model3.setDetailInfo(detailInfo3);
        models1.add(model3);

        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 4, 6);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo4 = Maps.newHashMap();
        model4.setDetailInfo(detailInfo4);
        models1.add(model4);

        analyzer.analyzeKeeperContainerUsedInfo(models1);

        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
        Assert.assertEquals("3.3.3.3", allDcReadyToMigrationKeeperContainers.get(0).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(0).getMigrateKeeperCount());
        Assert.assertEquals("4.4.4.4", allDcReadyToMigrationKeeperContainers.get(1).getTargetKeeperContainer().getKeeperIp());
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.get(1).getMigrateKeeperCount());
    }
}