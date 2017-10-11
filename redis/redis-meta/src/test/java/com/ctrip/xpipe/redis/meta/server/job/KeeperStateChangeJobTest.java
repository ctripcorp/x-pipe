package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
	
	@Mock
	private Command<?> activeSuccessCommand;
	
	
	@Before
	public void beforeKeeperStateChangeJobTest() throws Exception{
		
		keepers = new LinkedList<>();
		
		keepers = createRandomKeepers(2);
		
		
		job = new KeeperStateChangeJob(keepers, new Pair<>("localhost", randomPort()), getXpipeNettyClientKeyedObjectPool(), scheduled, executors);
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
			job.execute().get(100, TimeUnit.MILLISECONDS);
			Assert.fail();
		}catch(TimeoutException e){
		}
		
		verifyZeroInteractions(activeSuccessCommand);
	}

}
