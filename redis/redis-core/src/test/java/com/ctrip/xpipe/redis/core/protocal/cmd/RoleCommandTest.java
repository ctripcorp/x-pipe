package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.testutils.MemoryPrinter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;


/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
public class RoleCommandTest extends AbstractRedisTest{
	
	@Test
	public void test() throws Exception{
		
		SlaveRole role = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);
		
		Server server =  startServer(ByteBufUtils.readToString(role.format()));
		RoleCommand roleCommand = new RoleCommand(LOCAL_HOST, server.getPort(), scheduled);
		
		Role real = roleCommand.execute().get();

		logger.info("[test]{}", real);
		Assert.assertEquals(role, real);
	}
	
	@Test
	public void testNettyThreadLocalHoldingObjects() throws Exception {
		SlaveRole role = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0L);

		Server server =  startServer(ByteBufUtils.readToString(role.format()));

		int tasks = 5;
		CountDownLatch latch = new CountDownLatch(tasks);
		long before = MemoryPrinter.getFreeMemory();
		for(int i = 0; i < tasks; i++) {
			RoleCommand roleCommand = new RoleCommand(LOCAL_HOST, server.getPort(), scheduled);
			roleCommand.execute().addListener(new CommandFutureListener<Role>() {
				@Override
				public void operationComplete(CommandFuture<Role> commandFuture) throws Exception {
					latch.countDown();
				}
			});
		}
		latch.await();
		long after = MemoryPrinter.getFreeMemory();
		logger.info("[delta]{}", after - before);
	}
}
