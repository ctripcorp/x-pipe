package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public class PingCommandTest extends AbstractRedisTest{
	
	@Test
	public void testPing() throws Exception {
		
		FakeRedisServer fakeRedisServer = startFakeRedisServer();
		
		PingCommand command = new PingCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress("localhost", fakeRedisServer.getPort())), scheduled);
		String result = command.execute().get();
		Assert.assertEquals(PingCommand.PONG, result);
	}

	@Test
	public void testTimeout() throws Exception {

		PingCommand command = new PingCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress("10.2.55.174", 6379)), scheduled);
		String result = command.execute().get();
		Assert.assertEquals(PingCommand.PONG, result);

		sleep(30000);

		logger.info("[testTimeout][begin]");
		command = new PingCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new InetSocketAddress("10.2.55.174", 6379)), scheduled);
		result = command.execute().get();
		logger.info("[testTimeout][end]{}", result);

	}

}
