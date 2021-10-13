package com.ctrip.xpipe.redis.checker.alert;

import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
public class AlertManagerTest {

    @Mock
    private AlertConfig alertConfig;

    @Mock
    private AlertDbConfig alertDbConfig;

    @Mock
    private PersistenceCache persistenceCache;

    @InjectMocks
    AlertManager alertManager = new AlertManager();

    @Before
    public void beforeAlertManagerTest() {
        MockitoAnnotations.initMocks(this);
        when(alertConfig.getNoAlarmMinutesForClusterUpdate()).thenReturn(15);
        when(persistenceCache.getClusterCreateTime(anyString())).thenReturn(DateTimeUtils.getHoursBeforeDate(new Date(), 1));
        when(alertConfig.getAlertWhileList()).thenReturn(Collections.emptySet());
        when(alertDbConfig.clusterAlertWhiteList()).thenReturn(Sets.newHashSet("cluster1"));
    }

    @Test
    public void testGenerateAlertMessage() {
        String cluster = null, shard = "", message = "test message";
        ALERT_TYPE type = ALERT_TYPE.ALERT_SYSTEM_OFF;
        String result = alertManager.generateAlertMessage(cluster, "jq", shard, type, message);
        String expected = "jq," + type.simpleDesc() + "," + message;
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testShouldAlert() {
        Map<String, Date> map = Maps.newHashMapWithExpectedSize(1);
        map.put("cluster", new Date());
//        alertManager.setClusterCreateTime(map);
        alertManager.refreshWhiteList();

        Assert.assertFalse(alertManager.shouldAlert("cluster"));

        Assert.assertTrue(alertManager.shouldAlert("test"));
    }

    @Test
    public void testShouldAlertNullCluster() {
        Assert.assertTrue(alertManager.shouldAlert(null));
    }

    @Test
    public void testAlertWhiteList() {
        when(alertConfig.getNoAlarmMinutesForClusterUpdate()).thenReturn(15);
//        alertManager.setClusterCreateTime(Collections.emptyMap());
        alertManager.refreshWhiteList();

        Assert.assertFalse(alertManager.shouldAlert("cluster1"));
        Assert.assertTrue(alertManager.shouldAlert("cluster2"));
    }

}