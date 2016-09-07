package com.ctrip.xpipe.redis.meta.server.job;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;

/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleNotifycationTaskTest extends AbstractMetaServerTest{
	
	@Mock
	private ConsoleService consoleService;
	
	private ConsoleNotifycationTask consoleNotifycationTask;
	
	
	@Before
	public void beforeConsoleNotifycationTaskTest() throws Exception{
		consoleNotifycationTask = new ConsoleNotifycationTask();
		doThrow(new Exception()).when(consoleService).keeperActiveChanged(anyString(), anyString(), anyString(), (KeeperMeta) anyObject());
		consoleNotifycationTask.setConsoleService(consoleService);
	}
	
	
	@Test
	public void testTask() throws Exception{

		Assert.assertFalse(consoleNotifycationTask.getThread().isAlive());

		consoleNotifycationTask.keeperActiveElected("cluster1", "shard1", new KeeperMeta());
		
		sleep(1000);
		Assert.assertTrue(consoleNotifycationTask.getThread().isAlive());

		consoleNotifycationTask.getThread().interrupt();
		sleep(100);
		Assert.assertFalse(consoleNotifycationTask.getThread().isAlive());
		
	}
}
