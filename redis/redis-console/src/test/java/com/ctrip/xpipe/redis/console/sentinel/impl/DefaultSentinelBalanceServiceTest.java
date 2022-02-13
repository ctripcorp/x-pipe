package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.cluster.SentinelType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.SentinelGroupModel;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.SentinelGroupService;
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
        SentinelGroupModel jqSetinelTbl = sentinelBalanceService.selectSentinel("jq", SentinelType.DR_CLUSTER);
        SentinelGroupModel oySetinelTbl = sentinelBalanceService.selectSentinel("oy", SentinelType.DR_CLUSTER);
        Assert.assertEquals(101L, jqSetinelTbl.getSentinelGroupId());
        Assert.assertEquals(102L, oySetinelTbl.getSentinelGroupId());
    }

    @Test
    public void testSelectMultiSentinels() {
        Map<Long, SentinelGroupModel> dcSentinels = sentinelBalanceService.selectMultiDcSentinels(SentinelType.DR_CLUSTER);
        Assert.assertEquals(3, dcSentinels.size());
        Assert.assertEquals(101L, dcSentinels.get(1L).getSentinelGroupId());
        Assert.assertEquals(102L, dcSentinels.get(2L).getSentinelGroupId());
        Assert.assertEquals(3L, dcSentinels.get(3L).getSentinelGroupId());
    }

    @Test
    public void testReBalanceBackupDcSentinels() throws Exception {
        sentinelBalanceService.rebalanceBackupDcSentinel("oy");
        SentinelBalanceTask task = sentinelBalanceService.getBalanceTask("oy",SentinelType.DR_CLUSTER);
        waitConditionUntilTimeOut(() -> task.future().isDone());
        Assert.assertEquals(0, task.getShardsWaitBalances());
        Assert.assertEquals(2, task.getTargetUsages());

        Map<String, SentinelUsageModel> sentinelUsageModelMap = sentinelService.getAllSentinelsUsage();
        Map<String, Long> sentinelUsage = sentinelUsageModelMap.get("oy").getSentinelUsages();
        for (Long usage: sentinelUsage.values()) {
            Assert.assertEquals(2L, usage.longValue());
        }
    }

    @Test
    public void testReBalanceSentinels() throws Exception {
        sentinelBalanceService.rebalanceDcSentinel("jq",SentinelType.DR_CLUSTER);
        SentinelBalanceTask task = sentinelBalanceService.getBalanceTask("jq" ,SentinelType.DR_CLUSTER);
        waitConditionUntilTimeOut(() -> task.future().isDone(), 10000000);
        Assert.assertEquals(0, task.getShardsWaitBalances());
        Assert.assertEquals(3, task.getTargetUsages());

        Map<String, SentinelUsageModel> sentinelUsageModelMap = sentinelService.getAllSentinelsUsage();
        Map<String, Long> sentinelUsage = sentinelUsageModelMap.get("jq").getSentinelUsages();
        for (Long usage: sentinelUsage.values()) {
            Assert.assertEquals(3L, usage.longValue());
        }
    }

}
