package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.keeper.elect.KeeperRoleAssigner;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Jan 4, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperStateChangeJobTest extends AbstractMetaServerTest{
	
	private KeeperStateChangeJob job;
	private List<KeeperMeta> keepers;
	private int delayBaseMilli = 200;
	private int retryTimes = 1;
	
	@Mock
	private Command<?> activeSuccessCommand;

	@Mock
	private DcMetaCache dcMetaCache;

	private final List<String> callOrder = Collections.synchronizedList(new ArrayList<>());

	@Before
	public void beforeKeeperStateChangeJobTest() throws Exception{
		
		keepers = new LinkedList<>();
		
		keepers = createRandomKeepers(2);

		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", randomPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);

		when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
			KeeperMeta keeperMeta = invocation.getArgument(0);
			KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
			keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
			keeperContainerMeta.setDiskType(keeperMeta.getKeeperContainerId() >= 2L ? "tfs-1" : "DEFAULT");
			return keeperContainerMeta;
		});
	}

	@Test
	public void testRoute() throws Exception {

		String routeInfo = "PROXYTCP://1.1.1.1:80,PROXYTCP://1.1.1.2:80 PROXYTLS://1.1.1.5:443,PROXYTLS://1.1.1.6:443";
		startServer(keepers.get(0).getPort(), new AbstractIoActionFactory() {
			@Override
			protected byte[] getToWrite(Object readResult) {
				String result = (String) readResult;
				if(result != null && result.indexOf(routeInfo) >= 0){
					return "+OK\r\n".getBytes();
				}
				return "-No RouteFound\r\n".getBytes();
			}
		});
		startServer(keepers.get(1).getPort(), new AbstractIoActionFactory() {
			@Override
			protected byte[] getToWrite(Object readResult) {
				String result = (String) readResult;
				if(result != null && result.indexOf(routeInfo) >= 0){
					return "-Bad Route\r\n".getBytes();
				}
				return "+OK\r\n".getBytes();
			}
		});

		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", randomPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);

		try {
			job.execute().get(2000, TimeUnit.MILLISECONDS);
			Assert.fail();
		}catch (ExecutionException e){
		}

		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", randomPort()),
				new RouteMeta().setRouteInfo(routeInfo),
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);
		job.execute().get(2000, TimeUnit.MILLISECONDS);

	}
	
	
	@Test
	public void testHookSuccess() throws Exception{
		
		startServer(keepers.get(0).getPort(), "+OK\r\n");
		startServer(keepers.get(1).getPort(), "+OK\r\n");
		
		job.setActiveSuccessCommand(activeSuccessCommand);

		job.execute().get(2000, TimeUnit.MILLISECONDS);
		
		verify(activeSuccessCommand).execute();
		
	}

	@Test
	public void testHookFail() throws InterruptedException, ExecutionException, TimeoutException{

		job.setActiveSuccessCommand(activeSuccessCommand);

		try{
			job.execute().get(delayBaseMilli, TimeUnit.MILLISECONDS);
			Assert.fail();
		}catch(TimeoutException e){
		}
		
		verifyNoMoreInteractions(activeSuccessCommand);
	}

	@Ignore
	@Test
	public void testTimeout() throws Exception {
		delayBaseMilli = 1000;
		retryTimes = 5;
		long start = System.nanoTime();
		job = new KeeperStateChangeJob(keepers,
				new Pair<>(getTimeoutIp(), randomPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);
		try {
			job.execute().sync();
		} catch (Exception e) {

		}
		logger.info("[duration] {}", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start));
	}

	@Test
	public void testBmActiveTwoTfsSetStatePrepareWithRoles() throws Exception {
		callOrder.clear();
		KeeperMeta bm = keeper(7101, 1L, 1, true);
		KeeperMeta highTfs = keeper(7102, 2L, 5, false);
		KeeperMeta lowTfs = keeper(7103, 3L, 1, false);
		List<KeeperMeta> shardKeepers = new LinkedList<>();
		shardKeepers.add(bm);
		shardKeepers.add(highTfs);
		shardKeepers.add(lowTfs);

		Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(bm, shardKeepers, dcMetaCache);
		Assert.assertEquals(KeeperState.PREPARE, roles.get(lowTfs));
		Assert.assertEquals(KeeperState.BACKUP, roles.get(highTfs));

		startKeeperServer(bm.getPort());
		startKeeperServer(highTfs.getPort());
		startKeeperServer(lowTfs.getPort());

		job = new KeeperStateChangeJob(shardKeepers,
				new Pair<>("localhost", randomPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors, roles);
		job.execute().get(5000, TimeUnit.MILLISECONDS);

		Assert.assertTrue(callOrder.contains(bm.getPort() + ":ACTIVE"));
		Assert.assertTrue(callOrder.contains(highTfs.getPort() + ":BACKUP"));
		Assert.assertTrue(callOrder.contains(lowTfs.getPort() + ":PREPARE"));
		Assert.assertFalse(callOrder.contains(lowTfs.getPort() + ":BACKUP"));
	}

	@Test
	public void testWithoutRolesFallbackNonActiveToBackup() throws Exception {
		callOrder.clear();
		KeeperMeta active = keeper(7111, 1L, 1, true);
		KeeperMeta nonActive = keeper(7112, 3L, 1, false);
		List<KeeperMeta> shardKeepers = new LinkedList<>();
		shardKeepers.add(active);
		shardKeepers.add(nonActive);

		startKeeperServer(active.getPort());
		startKeeperServer(nonActive.getPort());

		job = new KeeperStateChangeJob(shardKeepers,
				new Pair<>("localhost", randomPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors, null);
		job.execute().get(5000, TimeUnit.MILLISECONDS);

		Assert.assertTrue(callOrder.contains(active.getPort() + ":ACTIVE"));
		Assert.assertTrue(callOrder.contains(nonActive.getPort() + ":BACKUP"));
	}

	private void startKeeperServer(int port) throws Exception {
		startServer(port, new AbstractIoActionFactory() {
			@Override
			protected byte[] getToWrite(Object readResult) {
				String state = parseKeeperSetState((String) readResult);
				if (state != null) {
					callOrder.add(port + ":" + state);
				}
				return "+OK\r\n".getBytes();
			}
		});
	}

	private String parseKeeperSetState(String request) {
		if (request == null) {
			return null;
		}
		if (request.contains("setstate PREPARE")) {
			return "PREPARE";
		}
		if (request.contains("setstate ACTIVE")) {
			return "ACTIVE";
		}
		if (request.contains("setstate BACKUP")) {
			return "BACKUP";
		}
		return null;
	}

	private KeeperMeta keeper(int port, long keeperContainerId, Integer priority, boolean active) {
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setIp("127.0.0.1");
		keeperMeta.setPort(port);
		keeperMeta.setKeeperContainerId(keeperContainerId);
		keeperMeta.setPriority(priority);
		keeperMeta.setActive(active);
		return keeperMeta;
	}

}
