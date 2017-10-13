package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * @author marsqing
 *
 *         Dec 8, 2016 3:56:43 PM
 */
public class HickwallConfig extends AbstractConfigBean {

	public static final String KEY_HICKWALL_HOST_PORT = "hickwall.host.port";
	public static final String KEY_HICKWALL_QUEUE_SIZE = "hickwall.queue.size";

	public String getHickwallHostPort() {
		return getProperty(KEY_HICKWALL_HOST_PORT, "");
	}

	public int getHickwallQueueSize() {
		return getIntProperty(KEY_HICKWALL_QUEUE_SIZE, 100 * 1000);
	}

}
