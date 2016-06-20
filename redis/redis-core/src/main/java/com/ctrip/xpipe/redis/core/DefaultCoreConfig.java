/**
 * 
 */
package com.ctrip.xpipe.redis.core;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.stereotype.Component;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:08:01 PM
 */
@Component
public class DefaultCoreConfig implements CoreConfig {
	@Override
	public int getZkConnectionTimeoutMillis() {
		return 3000;
	}

	private AtomicReference<String> zkConnectionString = new AtomicReference<>();

	@Override
	public String getZkConnectionString() {
		// TODO
		return zkConnectionString.get() == null ? ("127.0.0.1:" + System.getProperty("zkPort", "2181")) : zkConnectionString.get();
	}

	public void setZkConnectionString(String zkConnectionString) {
		this.zkConnectionString.set(zkConnectionString);
	}

	@Override
	public int getZkCloseWaitMillis() {
		return 1000;
	}

	@Override
	public String getZkNamespace() {
		return "xpipe";
	}

	@Override
	public int getZkRetries() {
		return Integer.MAX_VALUE;
	}

	@Override
	public int getSleepMsBetweenRetries() {
		return 1000;
	}

	@Override
	public int getZkSessionTimeoutMillis() {
		return 5000;
	}

	@Override
	public String getZkLeaderLatchRootPath() {
		return "/keepers";
	}

}
