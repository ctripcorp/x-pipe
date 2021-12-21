package com.ctrip.xpipe.pool;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 *         Nov 8, 2016
 */
public class XpipeNettyClientKeyedObjectPoolTest extends AbstractTest {

	private int testCount = 10;
	private int maxPerKey = 4;

	private XpipeNettyClientKeyedObjectPool pool;

	private Server echoServer;
	
	@Before
	public void beforeXpipeNettyClientKeyedObjectPoolTest() throws Exception {
		
		pool = new XpipeNettyClientKeyedObjectPool(maxPerKey);
		LifecycleHelper.initializeIfPossible(pool);
		LifecycleHelper.startIfPossible(pool);
		add(pool);
		echoServer = startEchoServer();
		
	}

	@After
	public void afterXpipeNettyClientKeyedObjectPoolTest() throws Exception {
		if (echoServer != null) {
			echoServer.stop();
		}
	}

//		@Test
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
		Endpoint key = new DefaultEndPoint("localhost", echoServer.getPort());

		pool.setKeyPooConfig(0, 200, 500, 100);

		SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(key);

		NettyClient nettyClient1 = objectPool.borrowObject();
		NettyClient nettyClient2 = objectPool.borrowObject();

		waitConditionUntilTimeOut(()->nettyClient1.channel().isActive()
				&& nettyClient2.channel().isActive());
		waitConditionUntilTimeOut(() -> echoServer.getConnected() == 2);

		objectPool.returnObject(nettyClient1);
		objectPool.returnObject(nettyClient2);

		waitConditionUntilTimeOut(() -> echoServer.getConnected() == 0, 60000);

	}

	@Test
	public void testKeyPoolReuse() throws Exception{
		Endpoint key = new DefaultEndPoint("localhost", echoServer.getPort());

		Assert.assertEquals(0, echoServer.getConnected());
		
		SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(key);
		
		for (int i = 0; i < testCount; i++) {
			
			NettyClient client = objectPool.borrowObject();
			waitConditionUntilTimeOut(()->client.channel().isActive(), 1000);
			Assert.assertEquals(1, echoServer.getTotalConnected());
			objectPool.returnObject(client);
		}
	}

	@Test
	public void testSingleReuse() throws Exception {

		Assert.assertEquals(0, echoServer.getTotalConnected());

		for (int i = 0; i < testCount; i++) {
			
			Endpoint key = new DefaultEndPoint("localhost", echoServer.getPort());
			NettyClient client = pool.borrowObject(key);
			waitConditionUntilTimeOut(()-> echoServer.getTotalConnected() == 1, 1000);
			pool.returnObject(key, client);
		}
	}
	
//	@Test(expected = BorrowObjectException.class)
	public void testException() throws BorrowObjectException{
		
		pool.borrowObject(new DefaultEndPoint("localhost", randomPort()));
	}
	
	@Test
	public void testDispose() throws Exception{
		Endpoint key = new DefaultEndPoint("localhost", echoServer.getPort());
		
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

		Endpoint key = new DefaultEndPoint("localhost", echoServer.getPort());
		
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

	// testOnBorrow = true, testOnReturn = false
	@Test
	public void testReturnWithConnectClose() throws Exception {

		Endpoint key = localhostEndpoint(echoServer.getPort());

		Assert.assertEquals(0, echoServer.getConnected());
		NettyClient client1 = pool.borrowObject(key);

		Assert.assertEquals(1, pool.getObjectPool(key).getNumActive());
		waitConditionUntilTimeOut(()->client1.channel().isActive(), 1000);
		client1.channel().close();
		pool.returnObject(key, client1);
		Assert.assertEquals(1, pool.getObjectPool(key).getNumIdle());
		Assert.assertEquals(0, pool.getObjectPool(key).getNumActive());

		waitConditionUntilTimeOut(()->!client1.channel().isActive(), 1000);
		Assert.assertEquals(1, pool.getObjectPool(key).getNumIdle());
		NettyClient client2 = pool.borrowObject(key);
		Assert.assertNotEquals(client1.channel(), client2.channel());

	}

	@Test
	public void testMapMemLeak() throws Exception {
		Endpoint key = localhostEndpoint(echoServer.getPort());

		NettyClient client = pool.borrowObject(key);
		pool.returnObject(key, client);

		Assert.assertNotNull(pool.getClientPool(key));
		pool.clear(key);

		Assert.assertNull(pool.getClientPool(key));
	}
	
	
	@Override
	protected void doAfterAbstractTest() throws Exception {
		super.doAfterAbstractTest();
	}

}
