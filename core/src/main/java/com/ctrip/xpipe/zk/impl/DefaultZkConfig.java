package com.ctrip.xpipe.zk.impl;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;

import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkConfig;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class DefaultZkConfig implements ZkConfig{
	
	private int zkSessionTimeoutMillis = 5000;
	private int zkRetries = 10;
	

	@Override
	public int getZkConnectionTimeoutMillis() {
		return 3000;
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
		return zkRetries;
	}

	public void setZkRetries(int zkRetries) {
		this.zkRetries = zkRetries;
	}
	
	@Override
	public int getSleepMsBetweenRetries() {
		return 1000;
	}

	@Override
	public int getZkSessionTimeoutMillis() {
		return zkSessionTimeoutMillis;
	}
	
	public void setZkSessionTimeoutMillis(int zkSessionTimeoutMillis) {
		this.zkSessionTimeoutMillis = zkSessionTimeoutMillis;
	}

	@Override
	public int waitForZkConnectedMillis() {
		return 5000;
	}

	@Override
	public CuratorFramework create(String address) throws InterruptedException {

		Builder builder = CuratorFrameworkFactory.builder();
		builder.connectionTimeoutMs(getZkConnectionTimeoutMillis());
		builder.connectString(address);
		builder.maxCloseWaitMs(getZkCloseWaitMillis());
		builder.namespace(getZkNamespace());
		builder.retryPolicy(new RetryNTimes(getZkRetries(), getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(getZkSessionTimeoutMillis());
		builder.threadFactory(XpipeThreadFactory.create("Xpipe-ZK-" + address, true));
		
		CuratorFramework curatorFramework = builder.build();
		curatorFramework.start();
		curatorFramework.blockUntilConnected(waitForZkConnectedMillis(), TimeUnit.MILLISECONDS);
		return curatorFramework;
	}
	
}
