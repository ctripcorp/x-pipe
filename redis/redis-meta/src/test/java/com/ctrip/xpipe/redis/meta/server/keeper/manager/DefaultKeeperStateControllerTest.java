package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.command.TestCommand;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperRemoveMetric;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

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

	@Test
	public void testRemoveMetricTfsRelease() throws Exception {
		isBm = false;
		MetricData data = removeAndCaptureMetric(false, new TestCommand("success", sleepInterval), true);

		Assert.assertEquals(KeeperRemoveMetric.METRIC_REMOVE_KEEPER, data.getMetricType());
		Assert.assertEquals("TFS", data.getTags().get("keeperType"));
		Assert.assertEquals("release", data.getTags().get("op"));
		Assert.assertEquals("fs1", data.getTags().get("tfsId"));
		Assert.assertFalse(data.getTags().containsKey("status"));
		Assert.assertEquals(1, data.getValue(), 0);
		Assert.assertEquals(new HostPort("127.0.0.1", 6000), data.getHostPort());
	}

	@Test
	public void testRemoveMetricTfsPurgeFail() throws Exception {
		isBm = false;
		MetricData data = removeAndCaptureMetric(true, new TestCommand(new Exception("remove fail")), false);

		Assert.assertEquals("TFS", data.getTags().get("keeperType"));
		Assert.assertEquals("remove", data.getTags().get("op"));
		Assert.assertEquals("fs1", data.getTags().get("tfsId"));
		Assert.assertEquals(0, data.getValue(), 0);
	}

	@Test
	public void testRemoveMetricBm() throws Exception {
		isBm = true;
		MetricData data = removeAndCaptureMetric(false, new TestCommand("success", sleepInterval), false);

		Assert.assertEquals("BM", data.getTags().get("keeperType"));
		Assert.assertEquals("remove", data.getTags().get("op"));
		Assert.assertEquals("none", data.getTags().get("tfsId"));
		Assert.assertEquals(1, data.getValue(), 0);
	}

	private MetricData removeAndCaptureMetric(boolean purge, TestCommand command, boolean release) throws Exception {
		if (release) {
			releaseCommand = command;
		} else {
			deleteCommand = command;
		}
		MetricProxy metricProxy = mock(MetricProxy.class);
		DefaultKeeperStateController controller = new DefaultKeeperStateController() {
			@Override
			protected Command<?> createDeleteKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled, int removeKeeperSuccessTimeoutMilli) {
				return deleteCommand;
			}

			@Override
			protected Command<?> createReleaseKeeperCommand(KeeperContainerService keeperContainerService,
					KeeperTransMeta keeperTransMeta, ScheduledExecutorService scheduled, int removeKeeperSuccessTimeoutMilli) {
				return releaseCommand;
			}

			@Override
			protected boolean isBm(KeeperTransMeta keeperTransMeta) {
				return isBm;
			}

			@Override
			protected String getTfsFsId(KeeperMeta keeperMeta) {
				return "fs1";
			}

			@Override
			protected KeeperContainerService getKeeperContainerService(KeeperTransMeta keeperTransMeta) {
				return keeperContainerService;
			}
		};
		controller.setExecutors(executors);
		controller.setMetricProxy(metricProxy);
		LifecycleHelper.initializeIfPossible(controller);
		LifecycleHelper.startIfPossible(controller);
		add(controller);

		controller.removeKeeper(new KeeperTransMeta(getClusterDbId(), getShardDbId(),
				new KeeperMeta().setIp("127.0.0.1").setPort(6000)), purge);

		ArgumentCaptor<MetricData> captor = ArgumentCaptor.forClass(MetricData.class);
		verify(metricProxy, timeout(1000).times(1)).writeBinMultiDataPoint(captor.capture());
		return captor.getValue();
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
