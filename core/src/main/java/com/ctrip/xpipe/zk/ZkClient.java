package com.ctrip.xpipe.zk;

import org.apache.curator.framework.CuratorFramework;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author marsqing
 *
 *         Jun 16, 2016 12:05:46 PM
 */
public interface ZkClient{

	CuratorFramework get();

	void setZkAddress(String zkAddress);

	String getZkAddress();

}
