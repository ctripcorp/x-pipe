package com.ctrip.xpipe.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;


/**
 * @author wenchao.meng
 *
 * Aug 23, 2016
 */
public class EphemeralNodeCreatorTest extends AbstractTest{

	private ZkTestServer zkServer;
	private CuratorFramework client; 
	private EphemeralNodeCreator ephemeralNodeCreator;
	private String rawData = randomString();
	private String path;
	
	@Before
	public void beforeEphemeralNodeCreatorTest() throws Exception{
		
		initRegistry();
		startRegistry();
		
		zkServer = startRandomZk();
		
		ZkConfig config = new DefaultZkConfig();
		client = config.create(String.format("localhost:%d", zkServer.getZkPort()));
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
		
		for(int i=0;i<10;i++){
			
			logger.info(remarkableMessage("[testStartStop]{}"), i);
			
			Assert.assertNull(client.checkExists().forPath(path));;
			ephemeralNodeCreator.start();
			Assert.assertNotNull(client.checkExists().forPath(path));;
			ephemeralNodeCreator.stop();
		}
	}
	
	@Test(expected = EphemeralNodeCanNotReplaceException.class)
	public void testAlreadyExistNotReplace() throws Exception{
		
		client.create().forPath(path, randomString().getBytes());
		ephemeralNodeCreator.start();
		
	}

	@Test
	public void testAlreadyExistReplace() throws Exception{

		client.create().forPath(path, rawData.getBytes());
		Stat stat = client.checkExists().forPath(path);
		Assert.assertNotNull(stat);
		
		ephemeralNodeCreator.start();
		
		Stat stat1 = client.checkExists().forPath(path);
		
		Assert.assertNotNull(stat1);
		Assert.assertNotEquals(stat, stat1);

	}

	@Test
	public void testSucceedDeleted() throws Exception{
		
		ephemeralNodeCreator.start();
		
		for(int i=0;i<1;i++){
			
			Stat stat1 = client.checkExists().forPath(path);
			Assert.assertNotNull(stat1);
			client.delete().forPath(path);
			sleep(50);
			Stat stat2 = client.checkExists().forPath(path);
			Assert.assertNotNull(stat2);
			Assert.assertNotEquals(stat1, stat2);
		}
	}

	@Test
	//used for manual watch
	public void testSucceedDisConnected() throws Exception{
		
		ephemeralNodeCreator.start();
		zkServer.stop();
		zkServer.dispose();
		
		zkServer.initialize();
		zkServer.start();
		
	}
}
