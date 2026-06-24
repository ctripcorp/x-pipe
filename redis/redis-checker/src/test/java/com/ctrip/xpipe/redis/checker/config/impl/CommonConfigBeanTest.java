package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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

    private static class TestableCommonConfigBean extends CommonConfigBean {
        void useConfig(Config config) {
            setConfig(config);
        }
    }
}
