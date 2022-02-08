package com.ctrip.xpipe.redis.meta.server.cluster.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 * Jul 27, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ArrangeTaskTriggerTest extends AbstractMetaServerTest{
	
	@Mock
	private ArrangeTaskExecutor arrangeTaskExecutor;
	
	@Mock
	private ClusterServer clusterServer;

	private ArrangeTaskTrigger arrangeTaskTrigger; 
	
	
	@Before
	public void beforeArrangeTaskTriggerTest() throws Exception{
		
		arrangeTaskTrigger = new ArrangeTaskTrigger();
		arrangeTaskTrigger.initialize();
		
		add(arrangeTaskTrigger);
		
		arrangeTaskTrigger.setArrangeTaskExecutor(arrangeTaskExecutor);
		arrangeTaskTrigger.setScheduled(scheduled);
		
	}
	
	@Test
	public void testRestart() throws IOException{
		
		int timeout = 500;
		
		arrangeTaskTrigger.setWaitForRestartTimeMills(timeout);
		
		verify(arrangeTaskExecutor, times(0)).offer(any());
		
		arrangeTaskTrigger.serverDead(clusterServer);
		
		sleep(100);
		
		arrangeTaskTrigger.serverAlive(clusterServer);

		sleep(timeout * 2);

		verify(arrangeTaskExecutor, times(0)).offer(any());

		
		arrangeTaskTrigger.serverDead(clusterServer);
		sleep(timeout * 2);

		verify(arrangeTaskExecutor, times(1)).offer(any());

	}
	
	
	
}
