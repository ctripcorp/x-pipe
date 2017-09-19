package com.ctrip.xpipe.redis.console.health.ping;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author marsqing
 *
 *         Dec 8, 2016 12:00:03 PM
 */
@Component
public class DefaultPingService implements PingService, PingCollector {

	@Autowired
	private ConsoleConfig config;

	private ConcurrentMap<HostPort, Long> hostPort2LastPong = new ConcurrentHashMap<>();

	@Override
	public boolean isRedisAlive(HostPort hostPort) {
		Long lastPongTime = hostPort2LastPong.get(hostPort);
		long maxNoPongTime = 2 * config.getRedisReplicationHealthCheckInterval();
		return lastPongTime != null && System.currentTimeMillis() - lastPongTime < maxNoPongTime;
	}

	@Override
	public void collect(PingSampleResult result) {
		for (Entry<HostPort, Boolean> entry : result.getSlaveHostPort2Pong().entrySet()) {
			if (entry.getValue()) {
				hostPort2LastPong.put(entry.getKey(), System.currentTimeMillis());
			}
		}
	}

}
