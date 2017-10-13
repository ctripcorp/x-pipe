package com.ctrip.xpipe.redis.console.health.delay;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map.Entry;

/**
 * @author marsqing
 *
 *         Dec 2, 2016 11:46:05 AM
 */
@Component
@ConditionalOnProperty(name = { AbstractProfile.PROFILE_KEY }, havingValue = AbstractProfile.PROFILE_NAME_TEST)
public class EchoDelayCollector implements DelayCollector {

	private static final Logger log = LoggerFactory.getLogger(EchoDelayCollector.class);

	@Override
	public void collect(DelaySampleResult result) {
		String fmt = "{}ms ({}) {}";
		log.info(fmt, result.getMasterDelayNanos() / 1000000, "master", result.getMasterHostPort());

		for (Entry<HostPort, Long> entry : result.getSlaveHostPort2Delay().entrySet()) {
			log.info(fmt, entry.getValue() / 1000000, "slave", entry.getKey());
		}
	}

}
