package com.ctrip.xpipe.redis.core.client;


import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.simpleserver.Server;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class ClientTest extends AbstractRedisTest{
	
	@Test
	public void testConnectSuccess() throws Exception{
		
		Server server = startEchoServer();
		Client client = new Client(new InetSocketAddress("localhost", server.getPort()));
		Assert.assertEquals(0, server.getConnected());
		client.initialize();
		Assert.assertEquals(0, server.getConnected());
		client.start();
		sleep(100);
		Assert.assertEquals(1, server.getConnected());
		
		client.stop();
		sleep(100);
		Assert.assertEquals(0, server.getConnected());
		
	}

	@Test
	public void testConnectFail() throws Exception{

		int port = randomPort();
		Client client = new Client(new InetSocketAddress("localhost", port));
		client.initialize();
		client.start();
		
		Assert.assertEquals(false, client.isAlive());		
	}

	@Test
	public void testClose() throws Exception{

		Server server = startEchoServer();
		Client client = new Client(new InetSocketAddress("localhost", server.getPort()));
		client.initialize();
		client.start();
		sleep(100);
		Assert.assertEquals(1, server.getConnected());
		
		client.close();
		sleep(client.getReconnnectTimeMilli() + 100);
		Assert.assertEquals(1, server.getConnected());
	}

}
