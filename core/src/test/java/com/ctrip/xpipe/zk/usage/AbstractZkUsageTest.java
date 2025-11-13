package com.ctrip.xpipe.zk.usage;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.ZkConfig;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;

/**
 * @author wenchao.meng
 *
 * Aug 30, 2016
 */
public class AbstractZkUsageTest extends AbstractTest{
	
	protected CuratorFramework client;
	private String zkAddress = "10.2.38.87:2182";
	
	@Before
	public void beforeAbstractZkUsageTest() throws InterruptedException{
		
		ZkConfig zkConfig = new DefaultZkConfig(zkAddress);
		client = zkConfig.create();
	}
	
	@After
	public void afterAbstractZkUsageTest() {
		if (client != null) {
			try {
				client.close();
			} catch (Exception e) {
				logger.error("[afterAbstractZkUsageTest][close client]", e);
			}
			client = null;
		}
	}
	
	public CuratorFramework getClient() {
		return client;
	}

}
