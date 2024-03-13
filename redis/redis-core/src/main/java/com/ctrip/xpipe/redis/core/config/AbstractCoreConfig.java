package com.ctrip.xpipe.redis.core.config;

import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.zk.ZkConfig;
import io.netty.util.internal.ConcurrentSet;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:08:01 PM
 */
public class AbstractCoreConfig extends AbstractConfigBean implements CoreConfig {

	private AtomicReference<String> zkConnection = new AtomicReference<>();
	private AtomicReference<String> zkNameSpace = new AtomicReference<>();

	private Set<ConfigKeyListener> listeners = new ConcurrentSet<>();

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

	@Override
	public void onChange(String key, String oldValue, String newValue) {
		super.onChange(key, oldValue, newValue);
		for (ConfigKeyListener listener: listeners) {
			try {
				listener.onChange(key, newValue);
			} catch (Throwable th) {
				logger.info("[onChange][{}][{}] fail", key, newValue, th);
			}
		}
	}

	@Override
	public void addListener(ConfigKeyListener listener) {
		this.listeners.add(listener);
	}
}
