package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CommonConfigBeanTest extends AbstractRedisTest {

    private Map<String, String> properties;

    private CommonConfigBean commonConfigBean;

    @Before
    public void setupCommonConfigBeanTest() {
        properties = new HashMap<>();
        commonConfigBean = new TestableCommonConfigBean();
        ((TestableCommonConfigBean) commonConfigBean).useConfig(new Config() {
            @Override
            public String get(String key) {
                return properties.get(key);
            }

            @Override
            public String get(String key, String defaultValue) {
                return properties.getOrDefault(key, defaultValue);
            }

            @Override
            public void addConfigChangeListener(ConfigChangeListener configChangeListener) {
            }

            @Override
            public void removeConfigChangeListener(ConfigChangeListener configChangeListener) {
            }

            @Override
            public int getOrder() {
                return 0;
            }
        });
    }

    @Test
    public void testDelayMetricNewInstanceMinutesDefault() {
        Assert.assertEquals(15, commonConfigBean.getDelayMetricNewInstanceMinutes());
    }

    @Test
    public void testDelayMetricNewInstanceMinutesValidValue() {
        properties.put(CommonConfigBean.KEY_DELAY_METRIC_NEW_INSTANCE_MINUTES, "30");
        Assert.assertEquals(30, commonConfigBean.getDelayMetricNewInstanceMinutes());
    }

    @Test
    public void testDelayMetricNewInstanceMinutesClampZero() {
        properties.put(CommonConfigBean.KEY_DELAY_METRIC_NEW_INSTANCE_MINUTES, "0");
        Assert.assertEquals(1, commonConfigBean.getDelayMetricNewInstanceMinutes());
    }

    @Test
    public void testDelayMetricNewInstanceMinutesClampNegative() {
        properties.put(CommonConfigBean.KEY_DELAY_METRIC_NEW_INSTANCE_MINUTES, "-5");
        Assert.assertEquals(1, commonConfigBean.getDelayMetricNewInstanceMinutes());
    }

    @Test
    public void testDelayMetricNewInstanceMinutesClampTooLarge() {
        properties.put(CommonConfigBean.KEY_DELAY_METRIC_NEW_INSTANCE_MINUTES, "100000");
        Assert.assertEquals(24 * 60, commonConfigBean.getDelayMetricNewInstanceMinutes());
    }

    @Test
    public void testBeaconSentinelGrayIdcClusterTypesParseAndNormalize() {
        properties.put(CommonConfigBean.KEY_BEACON_SENTINEL_GRAY_IDC_CLUSTER_TYPES,
                "{\"SHARB\":[\"one_way\",\"Single_Dc\"],\"shaxy\":[\"LOCAL_DC\"]}");

        Map<String, Set<String>> parsed = commonConfigBean.getBeaconSentinelGrayIdcClusterTypes();

        Assert.assertTrue(parsed.containsKey("sharb"));
        Assert.assertTrue(parsed.get("sharb").contains("ONE_WAY"));
        Assert.assertTrue(parsed.get("sharb").contains("SINGLE_DC"));
        Assert.assertTrue(parsed.get("shaxy").contains("LOCAL_DC"));
    }

    @Test
    public void testBeaconSentinelGrayIdcClusterTypesCacheUntilChanged() {
        properties.put(CommonConfigBean.KEY_BEACON_SENTINEL_GRAY_IDC_CLUSTER_TYPES,
                "{\"jq\":[\"ONE_WAY\"]}");
        Map<String, Set<String>> first = commonConfigBean.getBeaconSentinelGrayIdcClusterTypes();
        Map<String, Set<String>> second = commonConfigBean.getBeaconSentinelGrayIdcClusterTypes();
        Assert.assertSame(first, second);

        properties.put(CommonConfigBean.KEY_BEACON_SENTINEL_GRAY_IDC_CLUSTER_TYPES,
                "{\"oy\":[\"SINGLE_DC\"]}");
        Map<String, Set<String>> third = commonConfigBean.getBeaconSentinelGrayIdcClusterTypes();
        Assert.assertNotSame(first, third);
        Assert.assertTrue(third.containsKey("oy"));
        Assert.assertFalse(third.containsKey("jq"));
    }

    @Test
    public void testBeaconSentinelGrayIdcClusterTypesInvalidateOnChange() {
        properties.put(CommonConfigBean.KEY_BEACON_SENTINEL_GRAY_IDC_CLUSTER_TYPES,
                "{\"jq\":[\"ONE_WAY\"]}");
        Map<String, Set<String>> first = commonConfigBean.getBeaconSentinelGrayIdcClusterTypes();

        commonConfigBean.onChange(CommonConfigBean.KEY_BEACON_SENTINEL_GRAY_IDC_CLUSTER_TYPES,
                "{\"jq\":[\"ONE_WAY\"]}", "{\"jq\":[\"ONE_WAY\"]}");
        Map<String, Set<String>> afterInvalidate = commonConfigBean.getBeaconSentinelGrayIdcClusterTypes();
        Assert.assertNotSame(first, afterInvalidate);
        Assert.assertEquals(first, afterInvalidate);
    }

    @Test
    public void testBeaconSentinelGrayIdcClusterTypesInvalidJsonReturnsEmpty() {
        properties.put(CommonConfigBean.KEY_BEACON_SENTINEL_GRAY_IDC_CLUSTER_TYPES, "{not-json");
        Assert.assertTrue(commonConfigBean.getBeaconSentinelGrayIdcClusterTypes().isEmpty());
    }

    private static class TestableCommonConfigBean extends CommonConfigBean {
        void useConfig(Config config) {
            setConfig(config);
        }
    }
}
