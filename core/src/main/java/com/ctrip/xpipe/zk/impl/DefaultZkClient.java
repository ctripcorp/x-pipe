package com.ctrip.xpipe.zk.impl;



import org.apache.curator.framework.CuratorFramework;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkConfig;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:05:57 PM
 */
public class DefaultZkClient extends AbstractLifecycle implements ZkClient {

	private ZkConfig zkConfig = new DefaultZkConfig();

	private CuratorFramework client;
	
	private String address;

	@Override
	protected void doStart() throws Exception {
		
		client= zkConfig.create(address);
	}

	@Override
	public CuratorFramework get() {
		return client;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public void setZkConfig(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
	}

}
