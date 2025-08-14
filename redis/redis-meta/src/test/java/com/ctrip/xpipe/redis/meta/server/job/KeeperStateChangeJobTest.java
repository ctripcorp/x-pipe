package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.AbstractIoActionFactory;
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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

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

}
