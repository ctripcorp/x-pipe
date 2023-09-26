package com.ctrip.xpipe.redis.console.keeper.impl;

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

    @Before
    public void before() {
        Mockito.when(config.getClusterDividedParts()).thenReturn(2);
        Map<String, KeeperContainerOverloadStandardModel> standards = Maps.newHashMap();
        standards.put("jq", new KeeperContainerOverloadStandardModel().setFlowOverload(10).setPeerDataOverload(10));
        Mockito.when(config.getKeeperContainerOverloadStandards()).thenReturn(standards);
    }

    @Test
    public void TestGetAllDcReadyToMigrationKeeperContainersWithBoth() {
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

        List<KeeperContainerUsedInfoModel> models2 = new ArrayList<>();
        KeeperContainerUsedInfoModel model3 = new KeeperContainerUsedInfoModel("3.3.3.3", "jq", 5, 5);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo3 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster3", "shard1"), new Pair<>(2L, 2L));
        detailInfo3.put(new DcClusterShard("jq", "cluster4", "shard2"), new Pair<>(3L, 3L));
        model3.setDetailInfo(detailInfo3);
        models2.add(model3);


        KeeperContainerUsedInfoModel model4 = new KeeperContainerUsedInfoModel("4.4.4.4", "jq", 6, 6);
        Map<DcClusterShard, Pair<Long, Long>> detailInfo4 = Maps.newHashMap();
        detailInfo3.put(new DcClusterShard("jq", "cluster1", "shard1"), new Pair<>(3L, 3L));
        detailInfo3.put(new DcClusterShard("jq", "cluster2", "shard2"), new Pair<>(3L, 3L));
        model4.setDetailInfo(detailInfo4);
        models2.add(model4);

        analyzer.updateKeeperContainerUsedInfo(0, models1);
        List<MigrationKeeperContainerDetailModel> allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(0, allDcReadyToMigrationKeeperContainers.size());

        analyzer.updateKeeperContainerUsedInfo(1, models2);
        allDcReadyToMigrationKeeperContainers = analyzer.getAllDcReadyToMigrationKeeperContainers();
        Assert.assertEquals(2, allDcReadyToMigrationKeeperContainers.size());
    }
}