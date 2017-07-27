package com.ctrip.xpipe.utils;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.simpleserver.Server;


/**
 * @author wenchao.meng
 *
 * Sep 25, 2016
 */
public class TcpPortCheckTest extends AbstractTest{
	
	@Test
	public void testCheck() throws Exception{
		
		int port = randomPort();
		
		Assert.assertFalse(new TcpPortCheck("localhost", port).checkOpen());
		
		Server server = startEchoServer(port);
		
		Assert.assertTrue(new TcpPortCheck("localhost", port).checkOpen());
		sleep(100);
		Assert.assertEquals(0, server.getConnected());
		
		
	}

}
