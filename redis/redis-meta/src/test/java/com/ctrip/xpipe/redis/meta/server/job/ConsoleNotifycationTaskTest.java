package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.console.ConsoleService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.Mockito;

import java.util.concurrent.RejectedExecutionException;

import static org.mockito.Mockito.*;


/**
 * @author wenchao.meng
 *
 * Sep 7, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsoleNotifycationTaskTest extends AbstractMetaServerTest{
	
	@Mock
	private ConsoleService consoleService;

	@Mock
	private DcMetaCache dcMetaCache;
	
	private ConsoleNotifycationTask consoleNotifycationTask;
	
	@Before
	public void beforeConsoleNotifycationTaskTest() throws Exception{
		
		consoleNotifycationTask = new ConsoleNotifycationTask();
		consoleNotifycationTask.initialize();
		
		consoleNotifycationTask.setConsoleService(consoleService);
		Mockito.when(dcMetaCache.clusterShardDbId2Name(getClusterDbId(), getShardDbId())).thenReturn(Pair.from(getClusterId(), getShardId()));
	}
	
	@Test
	public void testSuccess() throws Exception{

		int times = 100;
		for(int i=0;i<times;i++){
			consoleNotifycationTask.keeperActiveElected(getClusterDbId(), getShardDbId(), new KeeperMeta());
			sleep(10);
			verify(consoleService, times(i + 1)).keeperActiveChanged(anyString(), anyString(), anyString(), any(KeeperMeta.class));
		}
		
	}
	
	@Test(expected = RejectedExecutionException.class)
	public void testDispose() throws Exception{

		consoleNotifycationTask.dispose();
		consoleNotifycationTask.keeperActiveElected(getClusterDbId(), getShardDbId(), new KeeperMeta());
		
	}
	
	
	@Test
	public void testException() throws Exception{

		ConsoleNotifycationTask task = new ConsoleNotifycationTask(100);
		task.initialize();
		task.setConsoleService(consoleService);
		
		doThrow(new Exception()).when(consoleService).keeperActiveChanged(anyString(), anyString(), anyString(), any(KeeperMeta.class));

		task.keeperActiveElected(getClusterDbId(), getShardDbId(), new KeeperMeta());
		
		sleep(300);
		verify(consoleService, atLeast(2)).keeperActiveChanged(anyString(), anyString(), anyString(), any(KeeperMeta.class));
		
		task.dispose();
		
	}
	
	@After
	public void afterConsoleNotifycationTaskTest() throws Exception{
		LifecycleHelper.disposeIfPossible(consoleNotifycationTask);
	}
	
}
