package com.ctrip.xpipe.pool;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.Server;

/**
 * @author wenchao.meng
 *
 *         Nov 8, 2016
 */
public class XpipeNettyClientKeyedObjectPoolTest extends AbstractTest {

	private int testCount = 10;
	private int maxPerKey = 4;

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
	public void testIdleClose() throws Exception {

		Server  echoServer = startEchoServer();
		InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());

		pool.setKeyPooConfig(0, 200, 500, 100);

		SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(key);

		NettyClient nettyClient1 = objectPool.borrowObject();
		NettyClient nettyClient2 = objectPool.borrowObject();

		waitConditionUntilTimeOut(() -> echoServer.getConnected() == 2);

		objectPool.returnObject(nettyClient1);
		objectPool.returnObject(nettyClient2);

		waitConditionUntilTimeOut(() -> echoServer.getConnected() == 0, 60000);

	}

	@Test
	public void testKeyPoolReuse() throws Exception{
		
		Server echoServer = startEchoServer();
		InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());

		Assert.assertEquals(0, echoServer.getConnected());
		
		SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(key);
		
		for (int i = 0; i < testCount; i++) {
			
			NettyClient client = objectPool.borrowObject();
			sleep(10);
			Assert.assertEquals(1, echoServer.getTotalConnected());
			objectPool.returnObject(client);
		}
	}

	@Test
	public void testSingleReuse() throws Exception {

		Server echoServer = startEchoServer();

		Assert.assertEquals(0, echoServer.getTotalConnected());

		for (int i = 0; i < testCount; i++) {
			
			InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());
			NettyClient client = pool.borrowObject(key);
			sleep(10);
			Assert.assertEquals(1, echoServer.getTotalConnected());
			pool.returnObject(key, client);
		}
	}
	
	@Test(expected = BorrowObjectException.class)
	public void testException() throws BorrowObjectException{
		
		pool.borrowObject(new InetSocketAddress("localhost", randomPort()));
	}
	
	@Test
	public void testDispose() throws Exception{

		Server echoServer = startEchoServer();
		InetSocketAddress key = new InetSocketAddress("localhost", echoServer.getPort());
		
		Assert.assertEquals(0, echoServer.getConnected());
		for(int i=0; i < maxPerKey; i++){
			
			pool.borrowObject(key);
			int finalI = i;
			waitConditionUntilTimeOut(() -> echoServer.getConnected() == finalI +1);
		}
		
		LifecycleHelper.stopIfPossible(pool);
		LifecycleHelper.disposeIfPossible(pool);
		
		waitConditionUntilTimeOut(() -> echoServer.getConnected() == 0);
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
