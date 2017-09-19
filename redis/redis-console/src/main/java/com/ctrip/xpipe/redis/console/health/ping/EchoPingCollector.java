package com.ctrip.xpipe.redis.console.health.ping;

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
 *         Dec 6, 2016 6:35:57 PM
 */
@Component
@ConditionalOnProperty(name = { AbstractProfile.PROFILE_KEY }, havingValue = AbstractProfile.PROFILE_NAME_TEST)
public class EchoPingCollector implements PingCollector {

	private static final Logger log = LoggerFactory.getLogger(EchoPingCollector.class);

	@Override
	public void collect(PingSampleResult result) {
		for (Entry<HostPort, Boolean> entry : result.getSlaveHostPort2Pong().entrySet()) {
			log.info("{} is {}", entry.getKey(), entry.getValue() ? "online" : "offline");
		}
	}

}
