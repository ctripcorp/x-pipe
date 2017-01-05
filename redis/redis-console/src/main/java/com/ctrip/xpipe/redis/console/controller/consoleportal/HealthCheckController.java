package com.ctrip.xpipe.redis.console.controller.consoleportal;

import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.health.delay.DelayService;
import com.ctrip.xpipe.redis.console.health.ping.PingService;
import com.google.common.collect.ImmutableMap;

/**
 * @author shyin
 *
 * Jan 5, 2017
 */
@RestController
@RequestMapping("console")
public class HealthCheckController extends AbstractConsoleController{
	
	@Autowired
	private PingService pingService;
	@Autowired
	private DelayService delayService;
	
	@RequestMapping(value = "/redis/health/{redisIp}/{redisPort}", method = RequestMethod.GET)
	public Map<String, Boolean> isRedisHealth(@PathVariable String redisIp, @PathVariable int redisPort) {
		return ImmutableMap.of("isHealth", pingService.isRedisAlive(new HostPort(redisIp, redisPort)));
	}
	
	@RequestMapping(value = "/redis/delay/{redisIp}/{redisPort}", method = RequestMethod.GET)
	public Map<String, Long> getReplDelayMillis(@PathVariable String redisIp, @PathVariable int redisPort) {
		return ImmutableMap.of("delay", delayService.getDelay(new HostPort(redisIp, redisPort)));
	}
}
