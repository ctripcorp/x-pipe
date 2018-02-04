package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class AlertManagerTest {

    @Mock
    private ConsoleConfig consoleConfig;

    @InjectMocks
    AlertManager alertManager = new AlertManager();

    @Before
    public void beforeAlertManagerTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGenerateAlertMessage() throws Exception {
        String cluster = null, shard = "", message = "test message";
        ALERT_TYPE type = ALERT_TYPE.ALERT_SYSTEM_OFF;
        String result = alertManager.generateAlertMessage(cluster, "jq", shard, type, message);
        String expected = "jq," + type.simpleDesc() + "," + message;
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testShouldAlert() {
        when(consoleConfig.getNoAlarmMinutesForNewCluster()).thenReturn(15);
        Map<String, Date> map = Maps.newHashMapWithExpectedSize(1);
        map.put("cluster", new Date());
        alertManager.setClusterCreateTime(map);
        alertManager.setAlertClusterWhiteList(Sets.newHashSet());

        Assert.assertFalse(alertManager.shouldAlert("cluster"));

        Assert.assertTrue(alertManager.shouldAlert("test"));
    }
}