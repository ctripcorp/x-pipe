package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

public class ClusterServiceImplTest4 extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterService clusterService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test3.sql");
    }

    @Test
    public void testReBalanceSentinelsForce() {
        List<String> clusters = clusterService.reBalanceSentinels("jq", 1, false);
        Assert.assertEquals(1, clusters.size());
        clusters = clusterService.reBalanceSentinels("jq", 10, false);
        Assert.assertEquals(2, clusters.size());
    }

}
