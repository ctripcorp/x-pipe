package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.ZkConfig;
import com.ctrip.xpipe.zk.ZkTestServer;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Sep 5, 2016
 */
public class DefaultLeaderElectorTest extends AbstractTest{
	
	private ZkTestServer zkTestServer;
	private CuratorFramework client;
	
	@Before
	public void beforeDefaultLeaderElectorTest() throws InterruptedException{
		zkTestServer = startRandomZk();
		
		ZkConfig config = new DefaultZkConfig(String.format("localhost:%d", zkTestServer.getZkPort()));
		client = config.create();
		
	}
	
	@Test
	public void testElect() throws Exception{
		
		String zkPath = "/" + getTestName();
		ElectContext electContext = new ElectContext(zkPath, "hi");
		DefaultLeaderElector leaderElector = new DefaultLeaderElector(electContext, client);
		leaderElector.elect();
		
		Assert.assertNotNull(client.checkExists().forPath(zkPath));
	}

}
