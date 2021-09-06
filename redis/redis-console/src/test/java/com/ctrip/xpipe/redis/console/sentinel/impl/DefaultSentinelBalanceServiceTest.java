package com.ctrip.xpipe.redis.console.sentinel.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.SentinelUsageModel;
import com.ctrip.xpipe.redis.console.model.SetinelTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceTask;
import com.ctrip.xpipe.redis.console.service.SentinelService;
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
    private SentinelService sentinelService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/sentinel-balance-test.sql");
    }

    @Test
    public void testSelectSentinel() {
        SetinelTbl jqSetinelTbl = sentinelBalanceService.selectSentinel("jq");
        SetinelTbl oySetinelTbl = sentinelBalanceService.selectSentinel("oy");
        Assert.assertEquals(101L, jqSetinelTbl.getSetinelId());
        Assert.assertEquals(102L, oySetinelTbl.getSetinelId());
    }

    @Test
    public void testSelectMultiSentinels() {
        Map<Long, SetinelTbl> dcSentinels = sentinelBalanceService.selectMultiDcSentinels();
        Assert.assertEquals(3, dcSentinels.size());
        Assert.assertEquals(101L, dcSentinels.get(1L).getSetinelId());
        Assert.assertEquals(102L, dcSentinels.get(2L).getSetinelId());
        Assert.assertEquals(3L, dcSentinels.get(3L).getSetinelId());
    }

    @Test
    public void testReBalanceBackupDcSentinels() throws Exception {
        sentinelBalanceService.rebalanceBackupDcSentinel("oy");
        SentinelBalanceTask task = sentinelBalanceService.getBalanceTask("oy");
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
        sentinelBalanceService.rebalanceDcSentinel("jq");
        SentinelBalanceTask task = sentinelBalanceService.getBalanceTask("jq");
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
