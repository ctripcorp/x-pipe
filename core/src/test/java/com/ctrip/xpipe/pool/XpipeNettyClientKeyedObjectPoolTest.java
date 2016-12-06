package com.ctrip.xpipe.pool;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.Server;

/**
 * @author wenchao.meng
 *
 *         Nov 8, 2016
 */
public class XpipeNettyClientKeyedObjectPoolTest extends AbstractTest {

	private int testCount = 1000;
	private int maxPerKey = 12;

	private XpipeNettyClientKeyedObjectPool pool;
	
	@Before
	public void beforeXpipeNettyClientKeyedObjectPoolTest() throws Exception{
		
		pool = new XpipeNettyClientKeyedObjectPool(maxPerKey);
		LifecycleHelper.initializeIfPossible(pool);
		LifecycleHelper.startIfPossible(pool);
		add(pool);
		
	}

	//	@Test
	// try xmx32m and run
	public void testGc() throws Exception {

		while (true) {

			XpipeNettyClientKeyedObjectPool pool = new XpipeNettyClientKeyedObjectPool();
			LifecycleHelper.initializeIfPossible(pool);
			LifecycleHelper.startIfPossible(pool);
			LifecycleHelper.stopIfPossible(pool);
			LifecycleHelper.disposeIfPossible(pool);
			sleep(10);
		}
	}

	@Test
	public void testSingleReuse() throws Exception {

		Server echoServer = startEchoServer();

		Assert.assertEquals(0, echoServer.getConnected());

		for (int i = 0; i < testCount; i++) {
			
			InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());
			NettyClient client = pool.borrowObject(key);
			Assert.assertEquals(1, echoServer.getConnected());
			pool.returnObject(key, client);
		}
	}
	
	@Test(expected = BorrowObjectException.class)
	public void testException() throws BorrowObjectException{
		
		pool.borrowObject(new InetSocketAddress("localhost", randomPort()));
	}
	
	@Test
	public void testDispose() throws Exception{
		logger.info("[testDispose][test start]");
		Server echoServer = startEchoServer();
		InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());
		
		Assert.assertEquals(0, echoServer.getConnected());
		for(int i=0; i < maxPerKey; i++){
			
			pool.borrowObject(key);
			sleep(100);
			Assert.assertEquals(i + 1, echoServer.getConnected());
		}
		
		LifecycleHelper.stopIfPossible(pool);
		LifecycleHelper.disposeIfPossible(pool);
		
		sleep(2000);
		Assert.assertEquals(0, echoServer.getConnected());
	}
	
	@Test
	public void testMax() throws Exception{
		
		Server echoServer = startEchoServer();
		InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());
		
		Assert.assertEquals(0, echoServer.getConnected());
		for(int i=0; i < maxPerKey; i++){
			
			pool.borrowObject(key);
			sleep(100);
			Assert.assertEquals(i + 1, echoServer.getConnected());
		}

		try{
			pool.borrowObject(key);
			Assert.fail();
		}catch(BorrowObjectException e){
			
		}
		
	}
	
	@Override
	protected void doAfterAbstractTest() throws Exception {
		super.doAfterAbstractTest();
	}

}
