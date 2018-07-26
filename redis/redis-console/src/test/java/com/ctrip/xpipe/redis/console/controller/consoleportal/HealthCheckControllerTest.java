package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.junit.Assert.*;

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
        String cluster = "cluster", shard = "shard", redisIp = "127.0.0.1";
        int port = 6379;
        Map<String, String> map = controller.getHickwallAddress(cluster, shard, redisIp, port);
        logger.info("[map] {}", map);
    }
}