package com.ctrip.xpipe.zk;


import org.apache.curator.framework.CuratorFramework;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;


/**
 * @author wenchao.meng
 *
 * Aug 23, 2016
 */
public class EphemeralNodeCreatorTestManul extends AbstractTest{

	private CuratorFramework client; 
	private EphemeralNodeCreator ephemeralNodeCreator;
	private String rawData = randomString();
	private String path;
	
	@Before
	public void beforeEphemeralNodeCreatorTest() throws Exception{
		
		ZkConfig config = new DefaultZkConfig();
		client = config.create(String.format("10.2.38.87:2181"));
		path = "/" + getTestName();
		
		ephemeralNodeCreator = new EphemeralNodeCreator(client, path, rawData.getBytes(), new NodeTheSame() {
			
			@Override
			public boolean same(byte[] data) {
				return rawData.equals(new String(data));
			}
		});
	}
	
	@Test
	public void testStartStop() throws Exception{
		
		ephemeralNodeCreator.start();
		waitForAnyKeyToExit();
		
	}
}
