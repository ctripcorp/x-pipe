package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.server.FakeRedisServer;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public class PingCommandTest extends AbstractRedisTest{
	
	@Test
	public void testPing() throws Exception {
		
		FakeRedisServer fakeRedisServer = startFakeRedisServer();
		
		PingCommand command = new PingCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("localhost", fakeRedisServer.getPort())), scheduled);
		String result = command.execute().get();
		Assert.assertEquals(PingCommand.PONG, result);
	}

	@Test(expected = CommandTimeoutException.class)
	public void testTimeout() throws Throwable {

		Server server = startServer((String) null);

		PingCommand command = new PingCommand(getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1",
				server.getPort())), scheduled);

		try{
			String result = command.execute().get();
		}catch (ExecutionException e){
			throw e.getCause();
		}
	}

}
