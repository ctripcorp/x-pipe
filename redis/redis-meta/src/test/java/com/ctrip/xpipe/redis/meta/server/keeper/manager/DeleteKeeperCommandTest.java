package com.ctrip.xpipe.redis.meta.server.keeper.manager;


import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DeleteKeeperCommandTest extends AbstractMetaServerTest{
	
	@Mock
	private KeeperContainerService keeperContainerService;
	
	private DeleteKeeperCommand deleteKeeperCommand;
	
	private String clusterId = "cluster1", shardId = "shard1";
	private Long clusterDbId = 1L, shardDbId = 1L;
	
	private KeeperMeta keeperMeta;
	
	private int timeoutMilli = 1000;
	private int checkIntervalMili = 100;
	
	@Before
	public void beforeDeleteKeeperCommandTest(){

		keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(randomPort());
		
		deleteKeeperCommand = new DeleteKeeperCommand(keeperContainerService, 
				new KeeperTransMeta(clusterDbId, shardDbId, keeperMeta), scheduled, timeoutMilli, checkIntervalMili);
		
	}

	@Test
	public void testDeleteSuccess() throws InterruptedException, ExecutionException{
		
		deleteKeeperCommand.execute().get();
		
	}

	@Test
	public void testDeleteFail() throws Exception{
		
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(keeperMeta);
		
		startEchoServer(keeperMeta.getPort());
		
		try{
			deleteKeeperCommand.execute().get();
			Assert.fail();
		}catch(ExecutionException e){
			Assert.assertTrue(e.getCause() instanceof DeleteKeeperStillAliveException);
		}
	}

	@Test
	public void testDeleteWaitTimeoutThenSuccess() throws Exception{
		
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(keeperMeta);
		
		final Server server = startEchoServer(keeperMeta.getPort());
		
		scheduled.schedule(new AbstractExceptionLogTask() {
			
			@Override
			protected void doRun() throws Exception {
				LifecycleHelper.stopIfPossible(server);
				LifecycleHelper.disposeIfPossible(server);
				
			}
		}, checkIntervalMili/2, TimeUnit.MILLISECONDS);
		
		deleteKeeperCommand.execute().get();
	}

}
