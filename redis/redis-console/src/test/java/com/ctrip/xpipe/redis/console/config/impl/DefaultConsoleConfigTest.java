package com.ctrip.xpipe.redis.console.config.impl;

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

import java.util.List;
import java.util.Set;

/**
 * @author wenchao.meng
 * <p>
 * Aug 15, 2017
 */
public class DefaultConsoleConfigTest extends AbstractConsoleTest {

    private CombConsoleConfig consoleConfig;

    @Before
    public void beforeDefaultConsoleConfigTest() {
        consoleConfig = new CombConsoleConfig(new CheckConfigBean(FoundationService.DEFAULT),
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

}
