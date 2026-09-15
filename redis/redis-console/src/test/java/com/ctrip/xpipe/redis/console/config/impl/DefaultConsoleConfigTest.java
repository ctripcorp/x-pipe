package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.model.BeaconClusterRoute;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author wenchao.meng
 * <p>
 * Aug 15, 2017
 */
public class DefaultConsoleConfigTest extends AbstractConsoleTest {

    private Map<String, String> properties;

    private TestableCheckConfigBean checkConfigBean;

    private DefaultConsoleConfig consoleConfig;

    @Before
    public void beforeDefaultConsoleConfigTest() {
        properties = new HashMap<>();
        checkConfigBean = new TestableCheckConfigBean();
        checkConfigBean.useConfig(new Config() {
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
        consoleConfig = new DefaultConsoleConfig(checkConfigBean,
                new ConsoleConfigBean(FoundationService.DEFAULT),
                new DataCenterConfigBean(),
                new CommonConfigBean());
    }

    @Test
    public void testWhiteList() {

        Set<String> whiteList = consoleConfig.getAlertWhileList();

        Set<String> result = Sets.newHashSet("cluster1", "cluster2", "cluster3");

        Assert.assertEquals(result, whiteList);
    }

    @Test
    public void testGetBeaconOrgRoutes() {

        List<BeaconOrgRoute> orgRoutes = consoleConfig.getBeaconOrgRoutes();

        BeaconClusterRoute clusterRoute1 = new BeaconClusterRoute("beacon-1", "http://10.62.131.12:8080", 100);
        BeaconClusterRoute clusterRoute2 = new BeaconClusterRoute("beacon-2", "http://10.62.131.11:8080", 60);
        BeaconClusterRoute clusterRoute3 = new BeaconClusterRoute("beacon-3", "http://10.60.57.171:8080", 100);

        BeaconOrgRoute orgRoute1 = new BeaconOrgRoute(0L, Lists.newArrayList(clusterRoute1, clusterRoute2), 100);
        BeaconOrgRoute orgRoute2 = new BeaconOrgRoute(7L, Lists.newArrayList(clusterRoute3), 80);
        List<BeaconOrgRoute> expected = Lists.newArrayList(orgRoute1, orgRoute2);

        Assert.assertEquals(expected, orgRoutes);
    }

    @Test
    public void testKeeperDelayCheckEnabled() {
        Assert.assertFalse(checkConfigBean.isKeeperDelayCheckEnabled());
        Assert.assertEquals(checkConfigBean.isKeeperDelayCheckEnabled(), consoleConfig.isKeeperDelayCheckEnabled());

        properties.put(CheckConfigBean.KEY_KEEPER_DELAY_CHECK_ENABLED, "false");
        Assert.assertFalse(checkConfigBean.isKeeperDelayCheckEnabled());
        Assert.assertEquals(checkConfigBean.isKeeperDelayCheckEnabled(), consoleConfig.isKeeperDelayCheckEnabled());

        properties.put(CheckConfigBean.KEY_KEEPER_DELAY_CHECK_ENABLED, "true");
        Assert.assertTrue(checkConfigBean.isKeeperDelayCheckEnabled());
        Assert.assertEquals(checkConfigBean.isKeeperDelayCheckEnabled(), consoleConfig.isKeeperDelayCheckEnabled());
    }

    @Test
    public void testKeeperCapabilityRefreshInterval() {
        Assert.assertEquals(60000, consoleConfig.getKeeperCapabilityRefreshIntervalMilli());
        Assert.assertEquals(60000, checkConfigBean.getKeeperCapabilityRefreshIntervalMilli());

        properties.put(CheckConfigBean.KEY_KEEPER_CAPABILITY_REFRESH_INTERVAL_MILLI, "1234");
        Assert.assertEquals(1234, checkConfigBean.getKeeperCapabilityRefreshIntervalMilli());
        Assert.assertEquals(1234, consoleConfig.getKeeperCapabilityRefreshIntervalMilli());
    }

    private static class TestableCheckConfigBean extends CheckConfigBean {

        TestableCheckConfigBean() {
            super(FoundationService.DEFAULT);
        }

        void useConfig(Config config) {
            setConfig(config);
        }
    }

}
