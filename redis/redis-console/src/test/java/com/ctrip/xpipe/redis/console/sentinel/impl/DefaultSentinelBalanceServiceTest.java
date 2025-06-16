package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.SentinelGroupService;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/9/2
 */
public class DefaultSentinelBalanceServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultSentinelBalanceService sentinelBalanceService;

    @Autowired
    private SentinelGroupService sentinelService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/sentinel-balance-test.sql");
    }

    @Test
    public void testSelectSentinel() {
        SentinelGroupModel jqSetinelTbl = sentinelBalanceService.selectSentinel("jq", ClusterType.ONE_WAY, "");
        SentinelGroupModel oySetinelTbl = sentinelBalanceService.selectSentinel("oy", ClusterType.ONE_WAY, "");
        Assert.assertEquals(101L, jqSetinelTbl.getSentinelGroupId());
        Assert.assertEquals(102L, oySetinelTbl.getSentinelGroupId());
    }

    @Test
    public void testSelectMultiSentinels() {
        Map<Long, SentinelGroupModel> dcSentinels = sentinelBalanceService.selectMultiDcSentinels(ClusterType.ONE_WAY, "");
        Assert.assertEquals(3, dcSentinels.size());
        Assert.assertEquals(101L, dcSentinels.get(1L).getSentinelGroupId());
        Assert.assertEquals(102L, dcSentinels.get(2L).getSentinelGroupId());
        Assert.assertEquals(3L, dcSentinels.get(3L).getSentinelGroupId());
    }

    @Test
    public void testReBalanceBackupDcSentinels() throws Exception {
        sentinelBalanceService.rebalanceBackupDcSentinel("oy", "");
        SentinelBalanceTask task = sentinelBalanceService.getBalanceTask("oy",ClusterType.ONE_WAY, "");
        waitConditionUntilTimeOut(() -> task.future().isDone());
        // active: 2 102; inactive: 104
        Assert.assertEquals(2, task.getTotalActiveSize());
        Assert.assertEquals(0, task.getShardsWaitBalances());
        // sentinel2:4,sentinel102:0 -> sentinel2:3,sentinel102:3
        Assert.assertEquals(2, task.getTargetUsages());

        Map<String, SentinelUsageModel> sentinelUsageModelMap = sentinelService.getAllSentinelsUsage(null, true);
        Map<String, Long> sentinelUsage = sentinelUsageModelMap.get("oy").getSentinelUsages();

        Assert.assertEquals(2L, sentinelUsage.get(sentinelService.findById(2).getSentinelsAddressString()).longValue());
        Assert.assertEquals(2L, sentinelUsage.get(sentinelService.findById(102).getSentinelsAddressString()).longValue());
        // inactive 104 not involved
        Assert.assertEquals(0L, sentinelUsage.get(sentinelService.findById(104).getSentinelsAddressString()).longValue());
    }

    @Test
    public void testReBalanceSentinels() throws Exception {
        sentinelBalanceService.rebalanceDcSentinel("jq",ClusterType.ONE_WAY,"");
        SentinelBalanceTask task = sentinelBalanceService.getBalanceTask("jq" ,ClusterType.ONE_WAY, "");
        waitConditionUntilTimeOut(() -> task.future().isDone(), 10000000);
        // active: 1 101; inactive: 103
        Assert.assertEquals(2, task.getTotalActiveSize());
        Assert.assertEquals(0, task.getShardsWaitBalances());
        // sentinel1:6,sentinel101:0 -> sentinel1:3,sentinel101:3
        Assert.assertEquals(3, task.getTargetUsages());

        Map<String, SentinelUsageModel> sentinelUsageModelMap = sentinelService.getAllSentinelsUsage("", true);
        Map<String, Long> sentinelUsage = sentinelUsageModelMap.get("jq").getSentinelUsages();

        Assert.assertEquals(3L, sentinelUsage.get(sentinelService.findById(1).getSentinelsAddressString()).longValue());
        Assert.assertEquals(3L, sentinelUsage.get(sentinelService.findById(101).getSentinelsAddressString()).longValue());
        // inactive 104 not involved
        Assert.assertEquals(0L, sentinelUsage.get(sentinelService.findById(103).getSentinelsAddressString()).longValue());
    }

}
