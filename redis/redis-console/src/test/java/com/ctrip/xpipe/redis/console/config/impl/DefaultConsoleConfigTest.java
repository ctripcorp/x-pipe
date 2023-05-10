package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.console.config.model.BeaconClusterRoute;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 * <p>
 * Aug 15, 2017
 */
public class DefaultConsoleConfigTest extends AbstractConsoleTest {

    private final String beaconConfig = "[\n" + "    {\n" + "        \"org_id\": 0,\n"
        + "        \"cluster_routes\": [\n" + "            {\n" + "                \"name\": \"beacon-1\",\n"
        + "                 \"host\": \"http://10.62.131.12:8080\",\n" + "                 \"weight\": 100\n"
        + "            },\n" + "            {\n" + "                \"name\": \"beacon-2\",\n"
        + "                 \"host\": \"http://10.62.131.11:8080\",\n" + "                 \"weight\": 60\n"
        + "            }\n" + "        ],\n" + "        \"weight\": 100\n" + "    },\n" + "    {\n"
        + "        \"org_id\": 7,\n" + "        \"cluster_routes\": [\n" + "            {\n"
        + "                \"name\": \"beacon-3\",\n" + "                \"host\": \"http://10.60.57.171:8080\",\n"
        + "                \"weight\": 100\n" + "            }\n" + "        ],\n" + "        \"weight\": 80\n"
        + "    }\n" + "]";

    private ConsoleConfig consoleConfig;

    @Before
    public void beforeDefaultConsoleConfigTest() {
        consoleConfig = new DefaultConsoleConfig();
    }

    @Test
    public void testWhiteList() {
        System.setProperty(DefaultConsoleConfig.KEY_ALERT_WHITE_LIST, "cluster1, cluster2 ; cluster3");

        Set<String> whiteList = consoleConfig.getAlertWhileList();

        Set<String> result = Sets.newHashSet("cluster1", "cluster2", "cluster3");

        Assert.assertEquals(result, whiteList);
    }

    @Test
    public void testDcClusterWontMarkDown() {
        System.setProperty(DefaultConsoleConfig.KEY_DC_CLUSTER_WONT_MARK_DOWN,
            "FAT-AWS:cluster_shyin, FAT:cluster_shyin:300");

        Set<DcClusterDelayMarkDown> result = consoleConfig.getDelayedMarkDownDcClusters();

        Set<DcClusterDelayMarkDown> expected = Sets.newHashSet(
            new DcClusterDelayMarkDown().setDcId("FAT-AWS").setClusterId("cluster_shyin").setDelaySecond(3600),
            new DcClusterDelayMarkDown().setDcId("FAT").setClusterId("cluster_shyin").setDelaySecond(300));

        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetBeaconOrgRoutes() {
        System.setProperty(DefaultConsoleConfig.KEY_BEACON_ORG_ROUTE, beaconConfig);

        List<BeaconOrgRoute> orgRoutes = consoleConfig.getBeaconOrgRoutes();

        BeaconClusterRoute clusterRoute1 = new BeaconClusterRoute("beacon-1", "http://10.62.131.12:8080", 100);
        BeaconClusterRoute clusterRoute2 = new BeaconClusterRoute("beacon-2", "http://10.62.131.11:8080", 60);
        BeaconClusterRoute clusterRoute3 = new BeaconClusterRoute("beacon-3", "http://10.60.57.171:8080", 100);

        BeaconOrgRoute orgRoute1 = new BeaconOrgRoute(0L, Lists.newArrayList(clusterRoute1, clusterRoute2), 100);
        BeaconOrgRoute orgRoute2 = new BeaconOrgRoute(7L, Lists.newArrayList(clusterRoute3), 80);
        List<BeaconOrgRoute> expected = Lists.newArrayList(orgRoute1, orgRoute2);

        Assert.assertEquals(expected, orgRoutes);
    }

}
