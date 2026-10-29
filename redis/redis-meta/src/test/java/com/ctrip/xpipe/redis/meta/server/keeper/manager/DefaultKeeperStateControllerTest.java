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
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperStateControllerTest extends AbstractMetaServerTest{

	private DefaultKeeperStateController defaultKeeperStateController;

	private int sleepInterval = 0;

	private Supplier<Command<?>> addCommandSupplier;

	private TestCommand addCommand = new TestCommand("success", sleepInterval);

	@Mock
	private KeeperContainerService keeperContainerService;

	private TestCommand deleteCommand = new TestCommand("success", sleepInterval);

	private TestCommand releaseCommand = new TestCommand("success", sleepInterval);

	private boolean isBm;

	@Before
	public void beforeDefaultKeeperStateControllerTest() throws Exception{

		defaultKeeperStateController = new DefaultKeeperStateController(){
			@Override
			protected Command<?> createAddKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
					int addKeeperSuccessTimeoutMilli) {
				return addCommandSupplier.get();
			}

			@Override
			protected Command<?> createDeleteKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
					int removeKeeperSuccessTimeoutMilli) {
				return deleteCommand;
			}

			@Override
			protected Command<?> createReleaseKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled,
					int removeKeeperSuccessTimeoutMilli) {
				return releaseCommand;
			}

			@Override
			protected boolean isBm(KeeperTransMeta keeperTransMeta) {
				return isBm;
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
		addCommandSupplier = new Supplier<Command<?>>() {
			@Override
			public Command<?> get() {
				return addCommand;
			}
		};
		Assert.assertFalse(addCommand.isBeginExecute());

		defaultKeeperStateController.addKeeper(new KeeperTransMeta(getClusterDbId(), getShardDbId(), new KeeperMeta()));
		sleep(50);
		Assert.assertTrue(addCommand.isBeginExecute());

	}

	@Test
	public void testDeleteOnPurge() throws TimeoutException {

		isBm = false;
		Assert.assertFalse(deleteCommand.isBeginExecute());
		defaultKeeperStateController.removeKeeper(new KeeperTransMeta(getClusterDbId(), getShardDbId(), new KeeperMeta()), true);
		waitConditionUntilTimeOut(() -> deleteCommand.isBeginExecute(), 1000);
		Assert.assertFalse(releaseCommand.isBeginExecute());
	}

	@Test
	public void testDeleteOnBmEvenWithoutPurge() throws TimeoutException {

		isBm = true;
		Assert.assertFalse(deleteCommand.isBeginExecute());
		defaultKeeperStateController.removeKeeper(new KeeperTransMeta(getClusterDbId(), getShardDbId(), new KeeperMeta()), false);
		waitConditionUntilTimeOut(() -> deleteCommand.isBeginExecute(), 1000);
		Assert.assertFalse(releaseCommand.isBeginExecute());
	}

	@Test
	public void testReleaseOnTfsWithoutPurge() throws TimeoutException {

		isBm = false;
		Assert.assertFalse(releaseCommand.isBeginExecute());
		defaultKeeperStateController.removeKeeper(new KeeperTransMeta(getClusterDbId(), getShardDbId(), new KeeperMeta()), false);
		waitConditionUntilTimeOut(() -> releaseCommand.isBeginExecute(), 1000);
		Assert.assertFalse(deleteCommand.isBeginExecute());
	}

	/** A stale keeper found on the target port is only released on TFS, removed on BM. */
	@Test
	public void testAddKeeperCommandReleasesStaleKeeperOnlyOnTfs() {
		for (boolean bm : new boolean[]{true, false}) {
			DefaultKeeperStateController controller = new DefaultKeeperStateController() {
				@Override
				protected boolean isBm(KeeperTransMeta keeperTransMeta) {
					return bm;
				}
			};
			AddKeeperCommand command = (AddKeeperCommand) controller.createAddKeeperCommand(keeperContainerService,
					new KeeperTransMeta(getClusterDbId(), getShardDbId(), new KeeperMeta()), scheduled, 1000);
			Assert.assertEquals("bm=" + bm, !bm, command.isReleaseStaleKeeper());
		}
	}

	@Test
	public void testSequentialExecInsideShard() throws Exception {
		AtomicInteger counter = new AtomicInteger();
		addCommandSupplier = new Supplier<Command<?>>() {
			@Override
			public Command<?> get() {
				return new CountingCommand(counter, 100);
			}
		};
		int tasks = 100;
		for(int i = 0; i < tasks; i++) {
			defaultKeeperStateController.addKeeper(new KeeperTransMeta(getClusterDbId(), getShardDbId(), new KeeperMeta()));
		}
		sleep(250);
		Assert.assertTrue(counter.get() < tasks && counter.get() > 0);
	}

	@Test
	public void testSequentialExecBetweenShards() throws Exception {
		AtomicInteger counter = new AtomicInteger();
		addCommandSupplier = new Supplier<Command<?>>() {
			@Override
			public Command<?> get() {
				return new CountingCommand(counter, 100);
			}
		};
		int tasks = 100;
		for(int i = 0; i < tasks; i++) {
			defaultKeeperStateController.addKeeper(new KeeperTransMeta(getClusterDbId(), Math.abs(randomLong()), new KeeperMeta()));
		}
		sleep(250);
		Assert.assertEquals(counter.get(), tasks);
	}

}
