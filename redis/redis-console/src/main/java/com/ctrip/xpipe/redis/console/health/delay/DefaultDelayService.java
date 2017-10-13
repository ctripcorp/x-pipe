package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import org.springframework.stereotype.Component;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 * Jan 5, 2017
 */
@Component
public class DefaultDelayService implements DelayService, DelayCollector{

	private ConcurrentMap<HostPort, Long> hostPort2Delay = new ConcurrentHashMap<>();
	
	@Override
	public void collect(DelaySampleResult result) {
		hostPort2Delay.put(result.getMasterHostPort(), TimeUnit.NANOSECONDS.toMillis(result.getMasterDelayNanos()));
		
		for(Entry<HostPort, Long> entry : result.getSlaveHostPort2Delay().entrySet()) {
			if(entry.getValue() != null) {
				hostPort2Delay.put(entry.getKey(), TimeUnit.NANOSECONDS.toMillis(entry.getValue()));
			}
		}
	}

	@Override
	public long getDelay(HostPort hostPort) {
		return hostPort2Delay.getOrDefault(hostPort, TimeUnit.NANOSECONDS.toMillis(DefaultDelayMonitor.SAMPLE_LOST_AND_NO_PONG));
	}

}
