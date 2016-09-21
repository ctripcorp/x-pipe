package com.ctrip.xpipe.zk.manual;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.zk.EternalWatcher;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;

/**
 * @author wenchao.meng
 *
 * Sep 21, 2016
 */
public class EternalManual extends AbstractTest{
	
	private String address = System.getProperty("zk.addr", "localhost:2181");
	private CuratorFramework client;
	private String path;
	
	@Before
	public void beforeEternalManual() throws Exception{
		
		path = "/" + getTestName();
		DefaultZkConfig zkConfig = new DefaultZkConfig();
		zkConfig.setZkRetries(3);
		client = zkConfig.create(address);
		client.createContainers(path);
		
	}
	
	@Test
	public void test() throws Exception{
		
		EternalWatcher eternalWatcher = new EternalWatcher(client, new CuratorWatcher() {
			
			@Override
			public void process(WatchedEvent event) throws Exception {
				logger.info("[process]{}", event);
			}
		}, path);
		
		eternalWatcher.start();
		
		scheduler.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				
				logger.info("[doRun][setdata]");
				client.setData().forPath(path, randomString().getBytes());
				
			}
		}, 0, 5, TimeUnit.SECONDS);
		
		waitForAnyKeyToExit();
	}
}
