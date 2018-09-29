package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.Server;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Aug 16, 2016
 */
public class XpipeNettyClientPoolTest extends AbstractTest {

	private Server serverPort;

	@Before
	public void before() throws Exception {
		serverPort = startEchoServer();
	}

	@Test
	public void testDeadBorrow() throws Exception {

		XpipeNettyClientPool clientPool = new XpipeNettyClientPool(
				new DefaultEndPoint("127.0.0.1", serverPort.getPort()));

		clientPool.initialize();
		clientPool.start();

		NettyClient nettyClient = clientPool.borrowObject();
		Channel channel1 = nettyClient.channel();

		clientPool.returnObject(nettyClient);

		NettyClient nettyClient2 = clientPool.borrowObject();
		Assert.assertEquals(channel1, nettyClient2.channel());

		logger.info("[testDeadBorrow][close client]");

		channel1.close();
		nettyClient2 = clientPool.borrowObject();
		Assert.assertNotEquals(channel1, nettyClient2.channel());

	}

}
