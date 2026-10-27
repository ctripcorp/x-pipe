package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.api.config.Config;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    /** 角色采集开关与延迟开关相互独立，默认关闭。 */
    @Test
    public void testKeeperStateCheckEnabled() {
        Assert.assertFalse(checkConfigBean.isKeeperStateCheckEnabled());
        Assert.assertEquals(checkConfigBean.isKeeperStateCheckEnabled(), consoleConfig.isKeeperStateCheckEnabled());

        properties.put(CheckConfigBean.KEY_KEEPER_STATE_CHECK_ENABLED, "true");
        Assert.assertTrue(checkConfigBean.isKeeperStateCheckEnabled());
        Assert.assertEquals(checkConfigBean.isKeeperStateCheckEnabled(), consoleConfig.isKeeperStateCheckEnabled());
        Assert.assertFalse("不得联动延迟开关", checkConfigBean.isKeeperDelayCheckEnabled());

        properties.put(CheckConfigBean.KEY_KEEPER_STATE_CHECK_ENABLED, "false");
        Assert.assertFalse(checkConfigBean.isKeeperStateCheckEnabled());
    }

    @Test
    public void testKeeperCapabilityRefreshInterval() {
        Assert.assertEquals(60000, consoleConfig.getKeeperCapabilityRefreshIntervalMilli());
        Assert.assertEquals(60000, checkConfigBean.getKeeperCapabilityRefreshIntervalMilli());

        properties.put(CheckConfigBean.KEY_KEEPER_CAPABILITY_REFRESH_INTERVAL_MILLI, "1234");
        Assert.assertEquals(1234, checkConfigBean.getKeeperCapabilityRefreshIntervalMilli());
        Assert.assertEquals(1234, consoleConfig.getKeeperCapabilityRefreshIntervalMilli());
    }

    @Test
    public void testSupportSentinelBeaconByIdcClusterType() {
        Map<String, Set<String>> idcClusterTypes = new HashMap<>();
        idcClusterTypes.put("jq", new HashSet<>(Collections.singletonList("one_way")));
        DefaultConsoleConfig config = BeaconSentinelGrayTestConfigs.withIdcClusterTypes(idcClusterTypes);

        Assert.assertTrue(config.supportSentinelBeacon(1L, "other-cluster", "JQ", ClusterType.ONE_WAY));
        Assert.assertFalse(config.supportSentinelBeacon(1L, "other-cluster", "jq", ClusterType.SINGLE_DC));
        Assert.assertFalse(config.supportSentinelBeacon(1L, "other-cluster", "oy", ClusterType.ONE_WAY));
        // HETERO must be resolved to azGroupType before matching
        Assert.assertFalse(config.supportSentinelBeacon(1L, "other-cluster", "jq", ClusterType.HETERO));
    }

    @Test
    public void testSupportSentinelBeaconUnionOfGrayRules() {
        Map<String, Set<String>> idcClusterTypes = new HashMap<>();
        idcClusterTypes.put("jq", Collections.singleton(ClusterType.LOCAL_DC.name()));
        DefaultConsoleConfig config = new DefaultConsoleConfig(checkConfigBean,
                new ConsoleConfigBean(FoundationService.DEFAULT),
                new DataCenterConfigBean(),
                new StubCommonConfigBean(Collections.singleton("gray-cluster"),
                        Collections.singleton(9L), idcClusterTypes));

        Assert.assertTrue(config.supportSentinelBeacon(1L, "gray-cluster", "oy", ClusterType.ONE_WAY));
        Assert.assertTrue(config.supportSentinelBeacon(9L, "other-cluster", "oy", ClusterType.ONE_WAY));
        Assert.assertTrue(config.supportSentinelBeacon(1L, "other-cluster", "jq", ClusterType.LOCAL_DC));
        Assert.assertFalse(config.supportSentinelBeacon(1L, "other-cluster", "oy", ClusterType.ONE_WAY));
    }

    private static class StubCommonConfigBean extends CommonConfigBean {

        private final Set<String> grayClusters;
        private final Set<Long> grayBus;
        private final Map<String, Set<String>> idcClusterTypes;

        StubCommonConfigBean(Set<String> grayClusters, Set<Long> grayBus, Map<String, Set<String>> idcClusterTypes) {
            this.grayClusters = grayClusters;
            this.grayBus = grayBus;
            this.idcClusterTypes = idcClusterTypes;
        }

        @Override
        public Set<String> getBeaconSentinelGrayClusters() {
            return grayClusters;
        }

        @Override
        public Set<Long> getBeaconSentinelGrayBus() {
            return grayBus;
        }

        @Override
        public Map<String, Set<String>> getBeaconSentinelGrayIdcClusterTypes() {
            return idcClusterTypes;
        }
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
