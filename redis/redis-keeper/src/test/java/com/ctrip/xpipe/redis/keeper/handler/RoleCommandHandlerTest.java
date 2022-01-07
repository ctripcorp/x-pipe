package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
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
	private RedisMaster redisMaster;
	
	private String host;
	private int    port;
	private MASTER_STATE masterState = MASTER_STATE.REDIS_REPL_CONNECTED;
	private long masterOffset = -1;
	
	private RoleCommandHandler handler = new RoleCommandHandler();
	
	@Before
	public void beforeRoleCommandHandlerTest(){
		
		host = "localhost";
		port  = randomPort();
		
		when(redisClient.getRedisKeeperServer()).thenReturn(redisKeeperServer);
		when(redisKeeperServer.getRedisMaster()).thenReturn(redisMaster);
		when(redisKeeperServer.role()).thenReturn(SERVER_ROLE.KEEPER);
		when(redisMaster.masterEndPoint()).thenReturn(new DefaultEndPoint(host, port));
		when(redisMaster.getMasterState()).thenReturn(masterState);
	}
	
	
	@Test
	public void test(){

		final AtomicReference<ByteBuf> result = new AtomicReference<>();
		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				
				result.set((ByteBuf) invocation.getArguments()[0]);
				return null;
			}
		}).when(redisClient).sendMessage(any(ByteBuf.class));
		
		handler.doHandle(new String[0], redisClient);
		sleep(10);
		
		String real = ByteBufUtils.readToString(result.get());
		
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

}
