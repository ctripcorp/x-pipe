package com.ctrip.xpipe.command;


import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.retry.RetryDelay;

/**
 * @author wenchao.meng
 *
 * Jul 14, 2016
 */
public class CommandRetryWrapperTest extends AbstractTest{

	private int retryCount = 3;
	private String message = randomString();
	private int sleepBase = 100;

	@Test
	public void testSucccess() throws InterruptedException, ExecutionException{

		TestCommand command = new TestCommand(message);
		CommandRetryWrapper<String> wrapper = (CommandRetryWrapper<String>) CommandRetryWrapper.buildCountRetry(retryCount, new RetryDelay(sleepBase), command, scheduled);
		
		Assert.assertEquals(message, wrapper.execute().get());
	}
	
	@Test
	public void testRetry() throws CommandExecutionException{
		
		TestCommand command = new TestCommand(new Exception("just throw"));
		CommandRetryWrapper<String> wrapper = (CommandRetryWrapper<String>) CommandRetryWrapper.buildCountRetry(retryCount, new RetryDelay(sleepBase), command, scheduled);
		try {
			wrapper.execute().get();
			Assert.fail();
		} catch (InterruptedException | ExecutionException e) {
		}
		Assert.assertEquals(wrapper.getExecuteCount(), retryCount + 1);
		
	}
	
	@Test
	public void testRetryCancel(){

		TestCommand command = new TestCommand(new Exception("just throw"));
		CommandRetryWrapper<String> wrapper = (CommandRetryWrapper<String>) CommandRetryWrapper.buildCountRetry(retryCount, new RetryDelay(sleepBase), command, scheduled);
		
		final CommandFuture<String> future = wrapper.execute();
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				sleep(sleepBase);
				future.cancel(true);
			}
		}).start();

		try {
			future.get();
			Assert.fail();
		} catch (Exception e) {
		}
		
		Assert.assertTrue(wrapper.getExecuteCount() < (retryCount + 1));
	}
	
	@Test
	public void testRetryUntilTimeout(){
		
		int timeout = sleepBase * 10;
		TestCommand command = new TestCommand(new Exception("just throw"));
		CommandRetryWrapper<String> wrapper = (CommandRetryWrapper<String>) CommandRetryWrapper.buildTimeoutRetry(timeout, new RetryDelay(sleepBase), command, scheduled);
		
		long before = System.currentTimeMillis();
		try {
			wrapper.execute().get();
			Assert.fail();
		} catch (InterruptedException | ExecutionException e) {
		}
		
		long after = System.currentTimeMillis();
		Assert.assertTrue((after - before) >= timeout);
		
	}
}
