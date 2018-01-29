package com.ctrip.xpipe.redis.meta.server.keeper.manager;

import com.ctrip.xpipe.api.server.Server.SERVER_ROLE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.simpleserver.Server;
import com.google.common.util.concurrent.SettableFuture;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * @author wenchao.meng
 *
 * Sep 16, 2016
 */
@RunWith(MockitoJUnitRunner.class)
public class AddKeeperCommandTest extends AbstractMetaServerTest{
	
	@Mock
	private KeeperContainerService keeperContainerService;
	
	private int timeoutMilli = 1000;
	private int checkInterval = 100;
	private KeeperTransMeta keeperTransMeta;
	private AddKeeperCommand addKeeperCommand;
	
	private int keeperPort = randomPort();

	@Before
	public void beforeAddKeeperCommandTest(){
		
		KeeperMeta keeperMeta = new KeeperMeta();
		keeperMeta.setPort(keeperPort);
		keeperMeta.setIp("localhost");
		
		keeperTransMeta  = new KeeperTransMeta("clusterId", "shardId", keeperMeta);
		addKeeperCommand = new AddKeeperCommand(keeperContainerService, keeperTransMeta, scheduled, timeoutMilli, checkInterval);
	}

	@Test
	public void testCheckStateCommandNoDelay() throws Exception {

		int sleepTime = 2000;
		SlaveRole keeperRole = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECTED, 0);
		Server server = startServer(keeperPort, new Callable<String>() {
			@Override
			public String call() throws Exception {
				sleep(sleepTime);
				return ByteBufUtils.readToString(keeperRole.format());
			}
		});

		SettableFuture<Boolean> objectSettableFuture = SettableFuture.create();

		executors.execute(new AbstractExceptionLogTask() {
			@Override
			public void doRun() throws Exception {

				AddKeeperCommand.CheckStateCommand checkStateCommand = new AddKeeperCommand.CheckStateCommand(new KeeperMeta().setIp("127.0.0.1").setPort(server.getPort()), scheduled);
				checkStateCommand.doExecute();
				objectSettableFuture.set(true);
			}
		});

		//should return immediately
		objectSettableFuture.get(500, TimeUnit.MILLISECONDS);
	}
	
	@SuppressWarnings("unused")
	@Test
	public void testSuccess() throws Exception{

		SlaveRole keeperRole = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECTED, 0);
		Server server = startServer(keeperPort, ByteBufUtils.readToString(keeperRole.format()));
		
		SlaveRole real = addKeeperCommand.execute().get();
		Assert.assertEquals(keeperRole, real);
		
	}

	@Test
	public void testFailTimeout() throws Exception{

		SlaveRole slaveRole = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0);
		startServer(keeperPort, ByteBufUtils.readToString(slaveRole.format()));
		
		long begin = System.currentTimeMillis();
		
		try{
			addKeeperCommand.execute().get();
			Assert.fail();
		}catch(ExecutionException e){
			Assert.assertTrue(e.getCause() instanceof KeeperMasterStateNotAsExpectedException);
		}
		long end   = System.currentTimeMillis();
		
		long interval = end - begin;
		Assert.assertTrue(interval >= timeoutMilli);
		Assert.assertTrue(interval <= timeoutMilli * 2);
	}

	@Test
	public void testIoExceptionNotRetry() throws Exception{

		SlaveRole slaveRole = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0);
		final Server server = startServer(keeperPort, ByteBufUtils.readToString(slaveRole.format()));
				
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					sleep(timeoutMilli/2);
					LifecycleHelper.stopIfPossible(server);
					LifecycleHelper.disposeIfPossible(server);
				} catch (Exception e) {
					logger.error("[run]" + server, e);
				}
			}
		}).start();
		
		try{
			addKeeperCommand.execute().get();
			Assert.fail();
		}catch(ExecutionException e){
			Assert.assertTrue(ExceptionUtils.isSocketIoException(e));
		}

	}

	@Test
	public void testFailThenSuccess() throws Exception{

		
		startServer(keeperPort, new Callable<String>() {
			
			long startTime = System.currentTimeMillis();
			@Override
			public String call() throws Exception {
				
				SlaveRole slaveRole = null;
				if(System.currentTimeMillis() - startTime <= timeoutMilli/2){
					slaveRole = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECT, 0);
				}else{
					slaveRole = new SlaveRole(SERVER_ROLE.KEEPER, "localhost", randomPort(), MASTER_STATE.REDIS_REPL_CONNECTED, 0);
				}
				return ByteBufUtils.readToString(slaveRole.format());
			}
		});
		
		SlaveRole keeperRole = addKeeperCommand.execute().get();
		Assert.assertEquals(MASTER_STATE.REDIS_REPL_CONNECTED, keeperRole.getMasterState());
	}

}
