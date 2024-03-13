package com.ctrip.xpipe.zk;

import org.apache.curator.framework.CuratorFramework;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface ZkConfig {
	
	public static String DEFAULT_ZK_NAMESPACE = "xpipe";

	int getZkConnectionTimeoutMillis();

	int getZkCloseWaitMillis();

	String getZkNamespace();

	int getZkRetries();

	int getSleepMsBetweenRetries();

	int getZkSessionTimeoutMillis();
	
	int waitForZkConnectedMillis();

	String getZkAddress();

	void updateZkAddress(String address);

	CuratorFramework create() throws InterruptedException;

}
