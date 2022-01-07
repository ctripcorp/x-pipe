package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * @author wenchao.meng
 *
 * Sep 28, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ArrangeTaskExecutorTest extends AbstractMetaServerTest{
	
	private ArrangeTaskExecutor arrangeTaskExecutor;
	
	@Mock
	private CurrentClusterServer currentClusterServer;
	
	
	@Before
	public void beforeArrangeTaskExecutorTest(){
		arrangeTaskExecutor = new ArrangeTaskExecutor();
		arrangeTaskExecutor.setCurrentClusterServer(currentClusterServer);
	}
	
	
	@Test
	public void testStop() throws Exception{
		
		System.setProperty(ArrangeTaskExecutor.ARRANGE_TASK_EXECUTOR_START, "true");
		
		arrangeTaskExecutor.initialize();
		arrangeTaskExecutor.start();
		
		sleep(50);
		
		Assert.assertTrue(arrangeTaskExecutor.getTaskThread().isAlive());
		
		arrangeTaskExecutor.stop();

		waitConditionUntilTimeOut( () -> arrangeTaskExecutor.getTaskThread() == null, 1000);

	}

}
