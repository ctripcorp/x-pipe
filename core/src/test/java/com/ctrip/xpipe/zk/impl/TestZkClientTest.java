package com.ctrip.xpipe.zk.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.ZkTestServer;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author wenchao.meng
 *
 * Oct 10, 2016
 */
public class TestZkClientTest extends AbstractTest{
	
	@Test
	public void test() throws Exception{

		ZkTestServer zkTestServer = startRandomZk();
		
		TestZkClient testZkClient = new TestZkClient();
		testZkClient.setZkAddress(String.format("127.0.0.1:%d", zkTestServer.getZkPort()));
		
		Assert.assertNull(testZkClient.get());

		testZkClient.initialize();
		testZkClient.start();
		Assert.assertNotNull(testZkClient.get());
		
		testZkClient.stop();
		Assert.assertNull(testZkClient.get());
	}
}
