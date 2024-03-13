package com.ctrip.xpipe.zk.impl;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkConfig;
import org.apache.curator.framework.CuratorFramework;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:05:57 PM
 */
public class TestZkClient extends AbstractLifecycle implements ZkClient, TopElement {

	private volatile CuratorFramework client;
	
	public static final String ZK_ADDRESS_KEY = "zkAddress";
	
	private String address = System.getProperty(ZK_ADDRESS_KEY, "127.0.0.1:2181");

	private ZkConfig zkConfig = new DefaultZkConfig(address);
	
	protected void doInitialize() throws Exception {
		
	}
	
	@Override
	protected void doStart() throws Exception {
		
	}

	@Override
	protected void doStop() throws Exception {
		if(client != null){
			client.close();
			client = null;
		}
	}

	@Override
	public void onChange(String key, String val) {
		if (key.equalsIgnoreCase(com.ctrip.xpipe.config.ZkConfig.KEY_ZK_ADDRESS)) {
			this.zkConfig.updateZkAddress(val);
		}
	}
	
	@Override
	public synchronized CuratorFramework get() {
		
		if(!getLifecycleState().isStarted()){
			logger.info("[get][not startted, return null]");
			return null;
		}
				
		if(client != null){
			return client;
		}

		try {
			client = zkConfig.create();
			return client;
		} catch (InterruptedException e) {
			logger.error("[get]", e);
		}
		return client;
	}
	
	@Override
	public void setZkAddress(String address) {
		this.zkConfig.updateZkAddress(address);
	}
	
	@Override
	public String getZkAddress(){
		return this.zkConfig.getZkAddress();
	}
	
	public void setClient(CuratorFramework client) {
		this.client = client;
	}
	
	public void setZkConfig(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}
	
	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE + 1;
	}
}
