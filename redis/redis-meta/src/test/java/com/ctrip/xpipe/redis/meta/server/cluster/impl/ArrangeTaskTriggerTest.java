package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServerInfo;
import com.ctrip.xpipe.redis.meta.server.cluster.RemoteClusterServerFactory;


/**
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
public class ArrangeTaskTriggerTest extends AbstractMetaServerTest{
	
	@Before
	public void beforeArrangeTaskTriggerTest() throws Exception{
		
		initRegistry();
		startRegistry();
	}
	
	@Test
	public void testRestart() throws IOException{
		
		int timeout = 1000;
		
		ArrangeTaskExecutor arrangeTaskExecutor = getBean(ArrangeTaskExecutor.class);
		
		ArrangeTaskTrigger arrangeTaskTrigger = getBean(ArrangeTaskTrigger.class);
		arrangeTaskTrigger.setWaitForRestartTimeMills(1000);
		
		RemoteClusterServerFactory factory = getBean(RemoteClusterServerFactory.class);
		
		ClusterServer clusterServer = factory.createClusterServer(100, new ClusterServerInfo("localhost", randomPort()));

		sleep(100);//wait for init task
		long taskCount1 = arrangeTaskExecutor.getTotalTasks();
		
		arrangeTaskTrigger.serverDead(clusterServer);
		
		sleep(100);
		
		arrangeTaskTrigger.serverAlive(clusterServer);

		sleep(timeout * 2);

		long taskCount2 = arrangeTaskExecutor.getTotalTasks();
		Assert.assertEquals(taskCount1, taskCount2);

		
		arrangeTaskTrigger.serverDead(clusterServer);
		sleep(timeout * 2);
		long taskCount3 = arrangeTaskExecutor.getTotalTasks();
		
		Assert.assertEquals(1, taskCount3 - taskCount2);
	}
	
	
	
}
