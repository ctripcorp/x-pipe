package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.junit.Assert;
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

    @Autowired
    private ConsoleConfig config;

    @Test
    public void testGetHickwallAddress() {
        String cluster = "xpipe_function", shard = "shard1", redisIp = "10.2.55.174";
        int port = 6379;
        Map<String, String> map = controller.getHickwallAddress(cluster, shard, redisIp, port);
        Assert.assertEquals(map.get("addr"), "http://hickwall.qa.nt.ctripcorp.com/grafanav2/d/UR32kfjWz/fx-xpipe?fullscreen&orgId=1&from=now-1h&to=now&panelId=2&var-cluster=xpipe_function&var-shard=shard1&var-address=10.2.55.174:6379");
    }

    @Test
    public void testGetOutComingTrafficToPeerHickwallAddress() {
        String cluster = "xpipe_function", shard = "shard1", redisIp = "10.2.55.174";
        int port = 6379;
        Map<String, String> map = controller.getOutComingTrafficToPeerHickwallAddress(redisIp, port);
        Assert.assertEquals(map.get("addr"), "http://hickwall.qa.nt.ctripcorp.com/grafanav2/d/UR32kfjWz/fx-xpipe?fullscreen&orgId=1&from=now-1h&to=now&panelId=16&var-address=10.2.55.174:6379");
    }
    
    @Test
    public void testGetInComingTrafficFromPeerHickwallAddress() {
        String cluster = "xpipe_function", shard = "shard1", redisIp = "10.2.55.174";
        int port = 6379;
        Map<String, String> map = controller.getInComingTrafficFromPeerHickwallAddress(redisIp, port);
        Assert.assertEquals(map.get("addr"), "http://hickwall.qa.nt.ctripcorp.com/grafanav2/d/UR32kfjWz/fx-xpipe?fullscreen&orgId=1&from=now-1h&to=now&panelId=18&var-address=10.2.55.174:6379");
    }
    
    @Test
    public void testGetPeerSyncFullHickwallAddress() {
        String cluster = "xpipe_function", shard = "shard1", redisIp = "10.2.55.174";
        int port = 6379;
        Map<String, String> map = controller.getPeerSyncFullHickwallAddress(redisIp, port);
        Assert.assertEquals(map.get("addr"), "http://hickwall.qa.nt.ctripcorp.com/grafanav2/d/UR32kfjWz/fx-xpipe?fullscreen&orgId=1&from=now-1h&to=now&panelId=20&var-address=10.2.55.174:6379");
    }
    
    @Test
    public void testGetPeerSyncPartialHickwallAddress() {
        String cluster = "xpipe_function", shard = "shard1", redisIp = "10.2.55.174";
        int port = 6379;
        Map<String, String> map = controller.getPeerSyncPartialHickwallAddress(redisIp, port);
        Assert.assertEquals(map.get("addr"), "http://hickwall.qa.nt.ctripcorp.com/grafanav2/d/UR32kfjWz/fx-xpipe?fullscreen&orgId=1&from=now-1h&to=now&panelId=22&var-address=10.2.55.174:6379");
    }
}