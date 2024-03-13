package com.ctrip.xpipe.zk.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkConfig;
import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
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
	
	private int zkSessionTimeoutMillis = Integer.parseInt(System.getProperty("ZK.SESSION.TIMEOUT", "5000"));
	private int zkConnectionTimeoutMillis = Integer.parseInt(System.getProperty("ZK.CONN.TIMEOUT", "3000"));
	private int zkRetries = 3;
	private String zkNameSpace = System.getProperty(KEY_ZK_NAMESPACE, DEFAULT_ZK_NAMESPACE);
	private EnsembleProvider ensembleProvider;

	public DefaultZkConfig(String address) {
		this.ensembleProvider = new FixedEnsembleProvider(address, true);
	}

	@Override
	public void updateZkAddress(String address) {
		logger.info("[updateZkAddress] {} -> {}", this.ensembleProvider.getConnectionString(), address);
		this.ensembleProvider.setConnectionString(address);
	}

	@Override
	public String getZkAddress() {
		return this.ensembleProvider.getConnectionString();
	}

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
	public CuratorFramework create() throws InterruptedException {

		Builder builder = CuratorFrameworkFactory.builder();
		builder.connectionTimeoutMs(getZkConnectionTimeoutMillis());
		builder.ensembleProvider(this.ensembleProvider);
		builder.maxCloseWaitMs(getZkCloseWaitMillis());
		builder.namespace(getZkNamespace());
		builder.retryPolicy(new RetryNTimes(getZkRetries(), getSleepMsBetweenRetries()));
		builder.sessionTimeoutMs(getZkSessionTimeoutMillis());
		builder.threadFactory(XpipeThreadFactory.create("Xpipe-ZK-" + this.ensembleProvider.getConnectionString(), true));

		logger.info("[create]{}, {}", Codec.DEFAULT.encode(this), this.ensembleProvider.getConnectionString());
		CuratorFramework curatorFramework = builder.build();
		curatorFramework.start();
		curatorFramework.blockUntilConnected(waitForZkConnectedMillis(), TimeUnit.MILLISECONDS);
		
		return curatorFramework;
	}
	
}
