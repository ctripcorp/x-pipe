package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DeleteKeeperCommandTest extends AbstractMetaServerTest{
	
	@Mock
	private KeeperContainerService keeperContainerService;
	
	@Mock
	private CurrentMetaManager currentMetaManager;
	
	private DeleteKeeperCommand deleteKeeperCommand;
	
	private String clusterId = "cluster1", shardId = "shard1";
	
	private KeeperMeta keeperMeta;
	
	private int timeoutMilli = 1000;
	private int checkIntervalMili = 100;
	
	@Before
	public void beforeDeleteKeeperCommandTest(){

		keeperMeta = new KeeperMeta();
		keeperMeta.setIp("localhost");
		keeperMeta.setPort(randomPort());
		
		deleteKeeperCommand = new DeleteKeeperCommand(currentMetaManager, keeperContainerService, 
				new KeeperInstanceMeta(clusterId, shardId, keeperMeta), timeoutMilli, checkIntervalMili);
		
	}

	@Test
	public void testDeleteSuccess() throws InterruptedException, ExecutionException{
		
		deleteKeeperCommand.execute().get();
		
	}

	@Test
	public void testDeleteFail() throws InterruptedException, ExecutionException{
		
		List<KeeperMeta> keepers = new LinkedList<>();
		keepers.add(keeperMeta);
		
		when(currentMetaManager.getSurviveKeepers(clusterId, shardId)).thenReturn(keepers);
		try{
			deleteKeeperCommand.execute().get();
			Assert.fail();
		}catch(ExecutionException e){
			Assert.assertTrue(e.getCause() instanceof DeleteKeeperStillAliveException);
		}
	}
}
