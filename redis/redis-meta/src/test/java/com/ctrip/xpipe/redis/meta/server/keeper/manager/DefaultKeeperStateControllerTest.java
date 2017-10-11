package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.TestCommand;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperStateControllerTest extends AbstractMetaServerTest{
	
	private DefaultKeeperStateController defaultKeeperStateController;
	
	private TestCommand addCommand = new TestCommand("success", 0);
	
	@Mock
	private KeeperContainerService keeperContainerService; 

	private TestCommand deleteCommand = new TestCommand("success", 0);

	@Before
	public void beforeDefaultKeeperStateControllerTest() throws Exception{
		
		defaultKeeperStateController = new DefaultKeeperStateController(){
			@Override
			protected Command<?> createAddKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
					int addKeeperSuccessTimeoutMilli) {
				return addCommand;
			}
			
			@Override
			protected Command<?> createDeleteKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
					int removeKeeperSuccessTimeoutMilli) {
				return deleteCommand;
			}
			

			@Override
			protected KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
				return keeperContainerService;
			}
			
		};

		defaultKeeperStateController.setExecutors(executors);
		LifecycleHelper.initializeIfPossible(defaultKeeperStateController);
		LifecycleHelper.startIfPossible(defaultKeeperStateController);
		
		add(defaultKeeperStateController);
		
	}
	
	
	@Test
	public void testAdd(){
		
		Assert.assertFalse(addCommand.isBeginExecute());
		
		defaultKeeperStateController.addKeeper(new KeeperTransMeta(getClusterId(), getShardId(), new KeeperMeta()));
		sleep(10);
		Assert.assertTrue(addCommand.isBeginExecute());
		
	}

	@Test
	public void testDelete() throws TimeoutException {
	
		Assert.assertFalse(deleteCommand.isBeginExecute());
		defaultKeeperStateController.removeKeeper(new KeeperTransMeta(getClusterId(), getShardId(), new KeeperMeta()));
		waitConditionUntilTimeOut(() -> deleteCommand.isBeginExecute(), 1000);
	}

}
