package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *
 * Nov 15, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultPsyncTest extends AbstractRedisTest{
	
	private DefaultPsync defaultPsync;
	
	@Mock
	private ReplicationStoreManager replicationStoreManager;
	
	@Mock
	private ReplicationStore replicationStore;
	
	@Mock
	private MetaStore metaStore;
	
	@Before
	public void beforeDefaultPsyncTest() throws Exception{
		
		FakeRedisServer fakeRedisServer = startFakeRedisServer();
		Endpoint masterEndPoint = new DefaultEndPoint("localhost", fakeRedisServer.getPort());
		SimpleObjectPool<NettyClient> pool = NettyPoolUtil.createNettyPool(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
		
		when(replicationStoreManager.createIfNotExist()).thenReturn(replicationStore);
		when(replicationStore.getMetaStore()).thenReturn(metaStore);
		when(metaStore.getReplId()).thenReturn("?");
		when(replicationStore.getEndOffset()).thenReturn(-1L);
		defaultPsync = new DefaultPsync(pool, masterEndPoint, replicationStoreManager, scheduled);
		
	}
	
	@Test
	public void testRefullSync() throws IOException{
		
		when(replicationStore.isFresh()).thenReturn(false);
		
		defaultPsync.execute().addListener(new CommandFutureListener<Object>() {
			
			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					logger.error("[operationComplete]", commandFuture.cause());
				}
			}
		});
		
		sleep(1000);
		verify(replicationStoreManager).create();
	}
}
