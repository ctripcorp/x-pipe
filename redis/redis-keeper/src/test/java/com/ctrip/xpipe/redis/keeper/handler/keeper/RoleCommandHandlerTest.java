package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import com.ctrip.xpipe.redis.keeper.store.meta.DefaultMetaStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.*;

/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class RoleCommandHandlerTest extends AbstractRedisKeeperTest{
	
	@Mock
	private RedisClient redisClient;
	
	@Mock
	private RedisKeeperServer redisKeeperServer;

	@Mock
	private DefaultReplicationStore replicationStore;

	@Mock
	private DefaultMetaStore metaStore;
	
	@Mock
	private RedisMaster redisMaster;

	@Mock
	private RedisKeeperServerState keeperServerState;
	
	private String host;
	private int    port;
	private MASTER_STATE masterState = MASTER_STATE.REDIS_REPL_CONNECTED;
	private long masterOffset = -1;
	
	private RoleCommandHandler handler = new RoleCommandHandler();
	
	@Before
	public void beforeRoleCommandHandlerTest(){
		
		host = "localhost";
		port  = randomPort();
		
		when(redisClient.getRedisServer()).thenReturn(redisKeeperServer);
		lenient().when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);
		when(redisKeeperServer.role()).thenReturn(SERVER_ROLE.KEEPER);
		lenient().when(replicationStore.getMetaStore()).thenReturn(metaStore);
		lenient().when(redisKeeperServer.getReplicationStore()).thenReturn(replicationStore);
		lenient().when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint(host, port));
		lenient().when(redisMaster.getMasterState()).thenReturn(masterState);
	}

	private ByteBuf captureRoleResponse() {
		final AtomicReference<ByteBuf> result = new AtomicReference<>();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) {
				result.set((ByteBuf) invocation.getArguments()[0]);
				return null;
			}
		}).when(redisClient).sendMessage(any(ByteBuf.class));
		handler.doHandle(new String[0], redisClient);
		return result.get();
	}
	
	
	@Test
	public void test(){

		String real = ByteBufUtils.readToString(captureRoleResponse());
		
		String expected = String.format("*5\r\n+%s\r\n+%s\r\n:%d\r\n+%s\r\n:%d\r\n", 
				SERVER_ROLE.KEEPER.toString(), host, port, 
				masterState.getDesc(),
				masterOffset);
		Assert.assertEquals(expected, real);
		

		//reverse
		Object []reverse = new ArrayParser().read(Unpooled.wrappedBuffer(real.getBytes())).getPayload();
		SlaveRole slaveRole = new SlaveRole(reverse);
		Assert.assertEquals(SERVER_ROLE.KEEPER, slaveRole.getServerRole());
		Assert.assertEquals(host, slaveRole.getMasterHost());
		Assert.assertEquals(port, slaveRole.getMasterPort());
		Assert.assertEquals(masterState, slaveRole.getMasterState());
		Assert.assertEquals(masterOffset, slaveRole.getMasterOffset());
	}

	/**
	 * D34 / T-17.3: PREPARE must return a legal ROLE 5-tuple without touching Store.
	 */
	@Test
	public void testPrepareReturnsRoleWithoutStore() {
		String prepareHost = "10.0.0.1";
		int preparePort = 6380;
		when(redisKeeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
		when(keeperServerState.keeperState()).thenReturn(KeeperState.PREPARE);
		when(keeperServerState.getMaster()).thenReturn(new DefaultEndPoint(prepareHost, preparePort));
		when(redisKeeperServer.getRedisMaster()).thenReturn(null);

		String real = ByteBufUtils.readToString(captureRoleResponse());
		String expected = String.format("*5\r\n+%s\r\n+%s\r\n:%d\r\n+%s\r\n:%d\r\n",
				SERVER_ROLE.KEEPER.toString(), prepareHost, preparePort,
				MASTER_STATE.REDIS_REPL_NONE.getDesc(), -1L);
		Assert.assertEquals(expected, real);
		verify(redisKeeperServer, never()).getReplicationStore();

		Object[] reverse = new ArrayParser().read(Unpooled.wrappedBuffer(real.getBytes())).getPayload();
		SlaveRole slaveRole = new SlaveRole(reverse);
		Assert.assertEquals(SERVER_ROLE.KEEPER, slaveRole.getServerRole());
		Assert.assertEquals(prepareHost, slaveRole.getMasterHost());
		Assert.assertEquals(preparePort, slaveRole.getMasterPort());
		Assert.assertEquals(MASTER_STATE.REDIS_REPL_NONE, slaveRole.getMasterState());
		Assert.assertEquals(-1L, slaveRole.getMasterOffset());
	}

}
