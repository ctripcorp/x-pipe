package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jul 26, 2018
 */
public class HealthCheckControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private HealthCheckController controller;

    @Test
    public void testGetHickwallAddress() {
        String cluster = "xpipe_function", shard = "shard1", redisIp = "10.2.55.174";
        int port = 6379;
        Map<String, String> map = controller.getHickwallAddress(cluster, shard, redisIp, port);
        logger.info("[map] {}", map);
    }
    
    @Test
    public void testGetOneWayClusterHickwallAddress() {
        String cluster = "xpipe_function";
        int port = 6379;
        Map<String, String> map = controller.getClusterHickwallAddress(ClusterType.ONE_WAY.name(),cluster);
        logger.info("[map] {}", map);
    }

    @Test
    public void testGetBiDirectionClusterHickwallAddress() {
        String cluster = "xpipe_function";
        int port = 6379;
        Map<String, String> map = controller.getClusterHickwallAddress(ClusterType.BI_DIRECTION.name(),cluster);
        logger.info("[map] {}", map);
    }
}