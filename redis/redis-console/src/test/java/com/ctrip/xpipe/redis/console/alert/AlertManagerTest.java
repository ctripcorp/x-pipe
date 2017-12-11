package com.ctrip.xpipe.redis.console.alert;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class AlertManagerTest {

    AlertManager alertManager;

    @Before
    public void beforeAlertManagerTest() {
        alertManager = new AlertManager();
    }

    @Test
    public void testGenerateAlertMessage() throws Exception {
        String cluster = null, shard = "", message = "test message";
        ALERT_TYPE type = ALERT_TYPE.ALERT_SYSTEM_OFF;
        String result = alertManager.generateAlertMessage(cluster, "jq", shard, type, message);
        String expected = "jq," + type.simpleDesc() + "," + message;
        Assert.assertEquals(expected, result);
    }

}