package com.ctrip.xpipe.concurrent;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.command.TestCommand;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jan 3, 2017
 */
public class KeyedOneThreadTaskExecutorTest extends AbstractTest{
	
	private int sleepInterval = 100;
	private KeyedOneThreadTaskExecutor<String> keyed;
	
	@Before
	public void beforeKeyedOneThreadTaskExecutorTest(){
		 keyed = new KeyedOneThreadTaskExecutor<>(executors);
	}

	@Test
	public void testHang() throws TimeoutException, IOException {

		int threadCount = 10;
		int taskCount = threadCount;

		ExecutorService executorService = null;
		try{

			executorService = Executors.newFixedThreadPool(threadCount, XpipeThreadFactory.create("test-hang"));
			keyed = new KeyedOneThreadTaskExecutor<>(executorService);

			AtomicInteger completeCount = new AtomicInteger();

			for(int i=0; i<taskCount ;i++){

				int finalI = i;
				ParallelCommandChain parallelCommandChain = new ParallelCommandChain(executorService);
				parallelCommandChain.add(new TestCommand("success:" + i, sleepInterval));
				parallelCommandChain.future().addListener(new CommandFutureListener<Object>() {
					@Override
					public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
						logger.info("[operationComplete]{}", finalI);
						completeCount.incrementAndGet();
					}
				});
				keyed.execute(String.valueOf(i), parallelCommandChain);
			}

			waitConditionUntilTimeOut(() -> completeCount.get() == taskCount);
		}finally {
			executorService.shutdownNow();
		}
	}



	@Test
	public void testSameKey() throws TimeoutException {
		TestCommand command1 =  new TestCommand("success", sleepInterval);
		TestCommand command2 =  new TestCommand("success", sleepInterval);
		
		keyed.execute("key1", command1);
		keyed.execute("key1", command2);

		waitConditionUntilTimeOut(()->assertSuccess(()->{
			Assert.assertTrue(command1.isBeginExecute());
			Assert.assertFalse(command2.isBeginExecute());
		}));
	}

	@Test
	public void testDifferentKey() throws TimeoutException {
		TestCommand command1 =  new TestCommand("success", sleepInterval);
		TestCommand command2 =  new TestCommand("success", sleepInterval);
		
		keyed.execute("key1", command1);
		keyed.execute("key2", command2);
		
		waitConditionUntilTimeOut(()->assertSuccess(()->{
			Assert.assertTrue(command1.isBeginExecute());
			Assert.assertTrue(command2.isBeginExecute());
		}));
	}

	@After
	public void afterKeyedOneThreadTaskExecutorTest() throws Exception{
		 keyed.destroy();
	}
}
