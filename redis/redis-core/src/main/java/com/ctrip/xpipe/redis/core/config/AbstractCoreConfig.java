package com.ctrip.xpipe.redis.core.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.zk.ZkConfig;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:08:01 PM
 */
public class AbstractCoreConfig extends AbstractConfigBean implements CoreConfig {
	
	public static String KEY_ZK_ADDRESS  = "zk.address";
	public static String KEY_ZK_NAMESPACE  = "zk.namespace";

	private AtomicReference<String> zkConnection = new AtomicReference<>();
	private AtomicReference<String> zkNameSpace = new AtomicReference<>();
	

	@Override
	public String getZkConnectionString() {
		
		return getProperty(KEY_ZK_ADDRESS, zkConnection.get() == null ? "127.0.0.1:2181" : zkConnection.get());
	}

	public void setZkConnectionString(String zkConnectionString) {
		this.zkConnection.set(zkConnectionString);
	}

	@Override
	public String getZkNameSpace(){
		return getProperty(KEY_ZK_NAMESPACE, zkNameSpace.get() == null ? ZkConfig.DEFAULT_ZK_NAMESPACE:zkNameSpace.get());
	}
	
	public void setZkNameSpace(String zkNameSpace) {
		this.zkNameSpace.set(zkNameSpace);
	}
}
