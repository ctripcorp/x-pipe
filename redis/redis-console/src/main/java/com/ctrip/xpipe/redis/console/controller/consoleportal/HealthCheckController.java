package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayService;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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

    private static final String TEMPLATE = "panelId=2&var-dc=%s&var-cluster=%s&var-shard=%s&var-address=%s:%d";

    private static final String ENDCODE_TYPE = "UTF-8";

    @RequestMapping(value = "/redis/health/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Boolean> isRedisHealth(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("isHealth", pingService.isRedisAlive(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, Long> getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
        return ImmutableMap.of("delay", delayService.getDelay(new HostPort(redisIp, redisPort)));
    }

    @RequestMapping(value = "/redis/health/hickwall/{dc}/" + CLUSTER_NAME_PATH_VARIABLE + "/" + SHARD_NAME_PATH_VARIABLE + "/{redisIp}/{redisPort}", method = RequestMethod.GET)
    public Map<String, String> getHickwallAddress(@PathVariable String dcName, @PathVariable String clusterName, @PathVariable String shardName, @PathVariable String redisIp, @PathVariable int redisPort) {
        String prefix = config.getHickwallAddress();
        if (Strings.isEmpty(prefix)) {
            return ImmutableMap.of("addr", "");
        }
        String template = null;
        try {
            template = URLEncoder.encode(String.format(TEMPLATE, dcName, clusterName, shardName, redisIp, redisPort), ENDCODE_TYPE);
        } catch (UnsupportedEncodingException e) {
            logger.error("[getHickwallAddress]", e);
            return ImmutableMap.of("addr", "");
        }
        String url = prefix + template;
        return ImmutableMap.of("addr", url);
    }
}
