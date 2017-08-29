package com.ctrip.xpipe.zk.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class DefaultZkConfig implements ZkConfig{
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public static String KEY_ZK_NAMESPACE = "key_zk_namespace";
	
	private int zkSessionTimeoutMillis = 5000;
	private int zkConnectionTimeoutMillis = 3000;
	private int zkRetries = 3;
	private String zkNameSpace = System.getProperty(KEY_ZK_NAMESPACE, DEFAULT_ZK_NAMESPACE);
	
	@Override
	public int getZkConnectionTimeoutMillis() {
		return zkConnectionTimeoutMillis;
	}

	public void setZkConnectionTimeoutMillis(int zkConnectionTimeoutMillis) {
		this.zkConnectionTimeoutMillis = zkConnectionTimeoutMillis;
	}

	@Override
	public int getZkCloseWaitMillis() {
		return 1000;
	}

	@Override
	public String getZkNamespace() {
		return zkNameSpace;
	}
	
	public void setZkNameSpace(String zkNameSpace) {
		this.zkNameSpace = zkNameSpace;
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
		return 100;
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

		logger.info("[create]{}, {}", Codec.DEFAULT.encode(this), address);
		CuratorFramework curatorFramework = builder.build();
		curatorFramework.start();
		curatorFramework.blockUntilConnected(waitForZkConnectedMillis(), TimeUnit.MILLISECONDS);
		
		return curatorFramework;
	}
	
}
