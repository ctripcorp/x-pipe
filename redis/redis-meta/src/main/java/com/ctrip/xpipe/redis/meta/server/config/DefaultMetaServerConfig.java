/**
 * 
 */
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
	
	private String consoleAddress = System.getProperty("consoleAddress", "http://localhost:8080");

	@Override
	public String getConsoleAddress() {
		return consoleAddress;
	}

	public void setConsoleAddress(String consoleAddress) {
		this.consoleAddress = consoleAddress;
	}
}
