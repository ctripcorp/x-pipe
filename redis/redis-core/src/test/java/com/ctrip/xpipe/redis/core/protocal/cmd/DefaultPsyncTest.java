package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.mockito.Mockito.*;

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

	private FakeRedisServer fakeRedisServer;

	private Endpoint masterEndPoint;

	private SimpleObjectPool<NettyClient> pool;

	@Before
	public void beforeDefaultPsyncTest() throws Exception{
		
		fakeRedisServer = startFakeRedisServer();
		masterEndPoint = new DefaultEndPoint("localhost", fakeRedisServer.getPort());
		pool = NettyPoolUtil.createNettyPool(new DefaultEndPoint("localhost", fakeRedisServer.getPort()));
		
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

	@Test
	public void testKeeperPartialSync() throws Exception {
		String replId = RunidGenerator.DEFAULT.generateRunid();
		int offset = 100;
		Server redisServer = startServer(randomPort(), new Function<String, String>() {
			@Override
			public String apply(String s) {
				logger.info("[testKeeperPartialSync] {}", s);
				if (s.trim().equals("psync ? -2")) {
					return String.format("+CONTINUE %s %d\r\n", replId, offset);
				} else {
					return "+OK\r\n";
				}
			}
		});
		Endpoint redisEndpoint = new DefaultEndPoint("localhost", redisServer.getPort());
		PartialOnlyPsync partialOnlyPsync = new PartialOnlyPsync(NettyPoolUtil.createNettyPool(redisEndpoint), redisEndpoint, replicationStoreManager, scheduled);

		when(replicationStore.isFresh()).thenReturn(true);

		CountDownLatch latch = new CountDownLatch(1);
		partialOnlyPsync.addPsyncObserver(new PsyncObserver() {
			@Override
			public void onFullSync(long masterRdbOffset) {
			}
			@Override
			public void reFullSync() {
			}
			@Override
			public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {
			}
			@Override
			public void endWriteRdb() {
			}
			@Override
			public void onContinue(String requestReplId, String responseReplId) {
			}
			@Override
			public void onKeeperContinue(String replId, long beginOffset) {
				latch.countDown();
			}
			@Override
			public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
			}
		});
		partialOnlyPsync.execute().addListener(new CommandFutureListener<Object>() {

			@Override
			public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
				if(!commandFuture.isSuccess()){
					logger.error("[operationComplete]", commandFuture.cause());
				}
			}
		});

		latch.await(1000, TimeUnit.SECONDS);
		verify(replicationStore, times(1)).continueFromOffset(replId, offset);
	}

}
