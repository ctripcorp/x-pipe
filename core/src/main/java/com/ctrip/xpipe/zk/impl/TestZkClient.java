package com.ctrip.xpipe.zk.impl;




import org.apache.curator.framework.CuratorFramework;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:05:57 PM
 */
public class TestZkClient extends AbstractLifecycle implements ZkClient, TopElement {

	private volatile CuratorFramework client;
	private ZkConfig zkConfig = new DefaultZkConfig();
	
	private String address = System.getProperty("zkAddress", "localhost:2181");
	
	protected void doInitialize() throws Exception {
		
	}
	
	@Override
	protected void doStart() throws Exception {
		
	}

	
	@Override
	protected void doDispose() throws Exception {
		super.doDispose();
		if(client != null){
			client.close();
			client = null;
		}
	}
	
	@Override
	protected void doStop() throws Exception {
	}
	
	@Override
	public synchronized CuratorFramework get() {
		
		if(client != null){
			return client;
		}

		try {
			client = zkConfig.create(address);
			return client;
		} catch (InterruptedException e) {
			logger.error("[get]", e);
		}
		return client;
	}
	
	@Override
	public void setZkAddress(String address) {
		this.address = address;
	}
	
	@Override
	public String getZkAddress(){
		return this.address;
	}
	
	public void setClient(CuratorFramework client) {
		this.client = client;
	}
	
	public void setZkConfig(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}
	
	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}
}
