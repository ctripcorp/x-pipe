package com.ctrip.xpipe.zk.impl;


import com.ctrip.xpipe.api.lifecycle.Lifecycle;
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
public class DefaultZkClient extends AbstractLifecycle implements ZkClient, TopElement, Lifecycle {

	private ZkConfig zkConfig;

	private CuratorFramework client;

	public DefaultZkClient(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}
	
	protected void doInitialize() throws Exception {
		
	}
	
	@Override
	protected void doStart() throws Exception {
		
		logger.info("[doStart]{}", this.zkConfig.getZkAddress());
		client= zkConfig.create();
	}

	
	@Override
	protected void doStop() throws Exception {
		client.close();
	}

	@Override
	public void onChange(String key, String val) {
		if (key.equalsIgnoreCase(com.ctrip.xpipe.config.ZkConfig.KEY_ZK_ADDRESS)) {
			this.zkConfig.updateZkAddress(val);
		}
	}
	
	@Override
	public CuratorFramework get() {
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

	@Override
	public int getOrder() {
		return Ordered.HIGHEST_PRECEDENCE;
	}

}
