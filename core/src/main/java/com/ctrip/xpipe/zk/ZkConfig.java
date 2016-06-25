package com.ctrip.xpipe.zk;

import org.apache.curator.framework.CuratorFramework;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public interface ZkConfig {

	int getZkConnectionTimeoutMillis();

	int getZkCloseWaitMillis();

	String getZkNamespace();

	int getZkRetries();

	int getSleepMsBetweenRetries();

	int getZkSessionTimeoutMillis();
	
	int waitForZkConnectedMillis();
	
	CuratorFramework create(String address) throws InterruptedException;

}
