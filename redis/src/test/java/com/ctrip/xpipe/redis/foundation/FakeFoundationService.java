/**
 * 
 */
package com.ctrip.xpipe.redis.foundation;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.foundation.FoundationService;

/**
 * @author marsqing
 *
 *         Jun 15, 2016 7:27:34 PM
 */
public class FakeFoundationService implements FoundationService {

	private static Logger log = LoggerFactory.getLogger(FakeFoundationService.class);

	private static AtomicBoolean logged = new AtomicBoolean(false);

	public FakeFoundationService() {
		if (logged.compareAndSet(false, true)) {
			log.info("data center is {}", System.getProperty("idc", "jq"));
		}
	}

	@Override
	public String getDataCenter() {
		return System.getProperty("idc", "jq");
	}

}
