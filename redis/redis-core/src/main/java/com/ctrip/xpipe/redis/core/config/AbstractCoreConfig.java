package com.ctrip.xpipe.redis.core.config;

import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.xpipe.config.AbstractConfigBean;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:08:01 PM
 */
public class AbstractCoreConfig extends AbstractConfigBean implements CoreConfig {
	
	public static String KEY_ZK_ADDRESS  = "zk.address";

	private AtomicReference<String> zkConnectionString = new AtomicReference<>();

	@Override
	public String getZkConnectionString() {
		
		return getProperty(KEY_ZK_ADDRESS, zkConnectionString.get() == null ? "127.0.0.1:2181" : zkConnectionString.get());
	}

	public void setZkConnectionString(String zkConnectionString) {
		this.zkConnectionString.set(zkConnectionString);
	}

}
