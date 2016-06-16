/**
 * 
 */
package com.ctrip.xpipe.foundation;

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

	private static String dataCenter = "jq";

	public static void setDataCenter(String dataCenter) {
		FakeFoundationService.dataCenter = dataCenter;
	}

	public FakeFoundationService() {
		if (logged.compareAndSet(false, true)) {
			log.info("data center is {}", dataCenter);
		}
	}

	@Override
	public String getDataCenter() {
		return dataCenter;
	}

}
