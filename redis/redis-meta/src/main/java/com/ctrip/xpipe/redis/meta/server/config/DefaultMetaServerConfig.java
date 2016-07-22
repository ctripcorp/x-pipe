package com.ctrip.xpipe.redis.meta.server.config;


import org.springframework.stereotype.Component;

import com.ctrip.xpipe.redis.core.impl.AbstractCoreConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 11:50:44 AM
 */
@Component
public class DefaultMetaServerConfig extends AbstractCoreConfig implements MetaServerConfig {
	
	public static String KEY_CONSOLE_ADDRESS = "console.adress";
	
	private String consoleAddress = System.getProperty("consoleAddress");

	@Override
	public String getConsoleAddress() {
		return getProperty(consoleAddress, consoleAddress);
	}

	public void setConsoleAddress(String consoleAddress) {
		this.consoleAddress = consoleAddress;
	}

	@Override
	public int getMetaRefreshMilli() {
		return 5000;
	}
}
