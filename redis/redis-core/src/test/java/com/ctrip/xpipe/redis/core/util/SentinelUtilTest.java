package com.ctrip.xpipe.redis.core.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class SentinelUtilTest {

    private String monitorName = "test-monitor";

    private String activeDc = "SHAOY", backupDc = "SHAJQ";

    @Test
    public void getSentinelMonitorName() {
        Assert.assertEquals(monitorName + '+' + activeDc, SentinelUtil.getSentinelMonitorName(monitorName, activeDc));
    }

    @Test
    public void getSentinelMonitorDcAndName() {
        Assert.assertEquals(activeDc, SentinelUtil.getSentinelMonitorDcAndName(monitorName + '+' + activeDc).getKey());
        Assert.assertEquals(monitorName, SentinelUtil.getSentinelMonitorDcAndName(monitorName + '+' + activeDc).getValue());
    }

    @Test
    public void getSentinelMonitorDcAndNameWithDoublePlus() {
        String monitorName = "test+monitor";
        Assert.assertEquals(activeDc, SentinelUtil.getSentinelMonitorDcAndName(monitorName + '+' + activeDc).getKey());
        Assert.assertEquals(monitorName, SentinelUtil.getSentinelMonitorDcAndName(monitorName + '+' + activeDc).getValue());
    }
}