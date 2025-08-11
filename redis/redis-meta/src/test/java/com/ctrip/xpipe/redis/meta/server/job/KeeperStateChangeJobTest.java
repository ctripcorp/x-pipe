package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;

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

		job = spy(job);
		when(job.checkKeeperMaster(any())).thenReturn(true);

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
		job = spy(job);
		when(job.checkKeeperMaster(any())).thenReturn(true);
		job.execute().get(2000, TimeUnit.MILLISECONDS);

	}
	
	
	@Test
	public void testHookSuccess() throws Exception{
		
		startServer(keepers.get(0).getPort(), "+OK\r\n");
		startServer(keepers.get(1).getPort(), "+OK\r\n");
		
		job.setActiveSuccessCommand(activeSuccessCommand);

		job = spy(job);
		when(job.checkKeeperMaster(any())).thenReturn(true);

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
		
		verifyZeroInteractions(activeSuccessCommand);
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
	public void testCheckKeeperMaster_master() throws Exception {

		Server master = getServer("master", "127.0.0.1", 6479);
		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", master.getPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);
		job = spy(job);
		job.execute();
		verify(job,times(1)).checkKeeperMaster(any());
		Assert.assertTrue(job.checkKeeperMaster(new Pair<>("localhost", master.getPort())));
	}

	@Test
	public void testCheckKeeperMaster_slave() throws Exception {
		Server slave = getServer("slave", "127.0.0.1", 6379);
		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", slave.getPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);
		job = spy(job);
		job.execute();
		verify(job,times(1)).checkKeeperMaster(any());
		Assert.assertFalse(job.checkKeeperMaster(new Pair<>("localhost", slave.getPort())));
	}

	@Test
	public void testCheckKeeperMaster_BackupKeeper() throws Exception {
		Server keeper = getServer("keeper", "127.0.0.1", 6379);

		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", keeper.getPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);
		job = spy(job);
		when(job.getInfo(any(), any())).thenReturn(redisInfo("backup", 0));

		job.execute();
		verify(job,times(1)).checkKeeperMaster(any());
		Assert.assertFalse(job.checkKeeperMaster(new Pair<>("localhost", keeper.getPort())));
	}

	@Test
	public void testCheckKeeperMaster_ActiveKeeper() throws Exception {
		Server keeper = getServer("keeper", "127.0.0.1", 6379);

		Server master = getServer("master", "127.0.0.1", 6479);

		job = new KeeperStateChangeJob(keepers,
				new Pair<>("localhost", keeper.getPort()),
				null,
				getXpipeNettyClientKeyedObjectPool(),
				delayBaseMilli, retryTimes,
				scheduled, executors);
		job = spy(job);
		when(job.getInfo(any(), any())).thenReturn(redisInfo("active", master.getPort()));

		job.execute();
		verify(job,times(1)).checkKeeperMaster(any());
		verify(job,times(2)).getRole(any(), any());
		Assert.assertTrue(job.checkKeeperMaster(new Pair<>("localhost", keeper.getPort())));
	}

	private String redisInfo(String state, int port) {
		return "# Replication\r\n" +
				"state:" + state.toUpperCase() + "\r\n" +
				"master_host:127.0.0.1\r\n" +
				"master_port:" + port + "\r\n";
	}

	private Server getServer(String role, String ip, int port) throws Exception {
		return startServer("*3\r\n"
				+ "$6\r\n" + role + "\r\n"
				+ ":43\r\n"
				+ "*3\r\n"
				+ "$9\r\n" + ip + "\r\n"
				+ "$4\r\n" + port + "\r\n"
				+ "$1\r\n0\r\n");
	}

}
