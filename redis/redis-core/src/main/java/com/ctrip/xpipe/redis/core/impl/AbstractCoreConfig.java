package com.ctrip.xpipe.redis.core.impl;

import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.redis.core.CoreConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:08:01 PM
 */
public class AbstractCoreConfig implements CoreConfig {
	

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
	public String getZkLeaderLatchRootPath() {
		return "/keepers";
	}

}
