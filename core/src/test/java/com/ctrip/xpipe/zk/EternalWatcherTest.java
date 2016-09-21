package com.ctrip.xpipe.zk;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;


/**
 * @author wenchao.meng
 *
 * Sep 21, 2016
 */
public class EternalWatcherTest extends AbstractTest{
	
	private EternalWatcher eternalWatcher;
	private ZkTestServer zkTestServer;
	private CuratorFramework client;
	private String path;
	private AtomicInteger watchCount = new AtomicInteger();
	private int testCount = 20;
	
	
	@Before
	public void beforeEternalWatcherTest() throws Exception{
		
		zkTestServer = startRandomZk();
		path = "/" + getTestName();
		client = new DefaultZkConfig().create(String.format("localhost:%d", zkTestServer.getZkPort()));
		client.createContainers(path);
	}
	
	@Test
	public void testStart() throws Exception{
		
		Assert.assertEquals(0, watchCount.get());
		eternalWatcher = new EternalWatcher(client, new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				watchCount.incrementAndGet();
			}
		}, path);

		changePath();
		sleep(20);
		Assert.assertEquals(0, watchCount.get());
		
		eternalWatcher.start();
		
		for(int i =0;i<testCount;i++){
			changePath();
			sleep(20);
			Assert.assertEquals(i+1, watchCount.get());
		}
	}
	
	
	private void changePath() throws Exception {
		client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path + "/" + UUID.randomUUID());
	}

	@Test
	public void testStop() throws Exception{

		Assert.assertEquals(0, watchCount.get());
		eternalWatcher = new EternalWatcher(client, new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				watchCount.incrementAndGet();
			}
		}, path);

		changePath();
		sleep(20);
		Assert.assertEquals(0, watchCount.get());
		
		eternalWatcher.start();
		changePath();
		sleep(20);
		Assert.assertEquals(1, watchCount.get());
		
		eternalWatcher.stop();
		changePath();
		sleep(20);
		int current = watchCount.get();
		
		for(int i=0;i<testCount;i++){
			changePath();
			sleep(20);
			Assert.assertEquals(current, watchCount.get());
		}
	}
	
	@After
	public void afterEternalWatcherTest() throws Exception{
		client.close();
		eternalWatcher.stop();
	}
}
