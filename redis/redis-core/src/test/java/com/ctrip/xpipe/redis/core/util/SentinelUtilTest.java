package com.ctrip.xpipe.redis.core.util;

import org.junit.Assert;
import org.junit.Test;

public class SentinelUtilTest {

    private String monitorName = "test-monitor";

    private String clusterName = "cluster";

    private String activeDc = "SHAOY", backupDc = "SHAJQ";

    @Test
    public void getSentinelMonitorName() {
        Assert.assertEquals(clusterName + '+' + monitorName + '+' + activeDc, SentinelUtil.getSentinelMonitorName(clusterName, monitorName, activeDc));
    }

    @Test
    public void getSentinelMonitorDcAndName() {
        Assert.assertEquals(activeDc, SentinelUtil.getSentinelInfoFromMonitorName(monitorName + '+' + activeDc).getIdc());
        Assert.assertEquals(monitorName, SentinelUtil.getSentinelInfoFromMonitorName(monitorName + '+' + activeDc).getShardName());
    }

}