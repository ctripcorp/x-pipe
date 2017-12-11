package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.health.delay.DelayService;
import com.ctrip.xpipe.redis.console.health.ping.PingService;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @author shyin
 *         <p>
 *         Jan 5, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class HealthCheckController extends AbstractConsoleController {

    @Autowired
    private PingService pingService;
    @Autowired
    private DelayService delayService;
    @Autowired
    private ConsoleConfig config;

    @RequestMapping(value = "/redis/health/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Boolean> isRedisHealth(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("isHealth", pingService.isRedisAlive(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Long> getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("delay", delayService.getDelay(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/health/hickwall/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getHickwallAddress(@PathVariable String clusterName, @PathVariable String shardName, @PathVariable String redisIp, @PathVariable int redisPort) {
        String addr = config.getHickwallAddress();
        if (Strings.isEmpty(addr)) {
            return ImmutableMap.of("addr", "");
        }
        return ImmutableMap.of("addr", String.format("%s.%s.%s.%s.%s*", addr, clusterName, shardName, redisIp, redisPort));
    }
}
