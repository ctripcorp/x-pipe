package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.api.config.ConfigProvider;

/**
 * @author marsqing
 *
 *         Dec 8, 2016 3:56:43 PM
 */
public class HickwallConfig extends AbstractConfigBean {

	public static final String KEY_HICKWALL_QUEUE_SIZE = "hickwall.queue.size";
	public static final String KEY_HICKWALL_ADDRESS = "hickwall.address";
	public static final String KEY_HICKWALL_DATABASE = "hickwall.database";
	public static final String KEY_HICKWALL_BATCH_SIZE = "hickwall.batch.size";
	public static final String KEY_HICKWALL_WRITE_MONITOR = "hickwall.monitor.write";

	public HickwallConfig() {
		super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.DATA_CENTER_CONFIG_NAME));
	}

	public int getHickwallBatchSize() {
		return getIntProperty(KEY_HICKWALL_BATCH_SIZE, 1000);
	}

	public boolean getHickwallWriteMonitor() {
		return getBooleanProperty(KEY_HICKWALL_WRITE_MONITOR, false);
	}

	public int getHickwallQueueSize() {
		return getIntProperty(KEY_HICKWALL_QUEUE_SIZE, 100 * 1000);
	}

	public String getHickwallAddress() {
		return getProperty(KEY_HICKWALL_ADDRESS, "http://127.0.0.1:80");
	}

	public String getHickwallDatabase() {
		return getProperty(KEY_HICKWALL_DATABASE, "APM-FX");
	}

}
